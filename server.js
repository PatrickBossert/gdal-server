const express = require('express');
const multer = require('multer');
const cors = require('cors');
const gdal = require('gdal-async');
const fs = require('fs').promises;
const path = require('path');

const app = express();

// Port configuration for Railway
let port = 3001;
if (process.env.PORT) {
  const envPort = parseInt(process.env.PORT, 10);
  if (envPort >= 0 && envPort <= 65535) {
    port = envPort;
  }
}

// Middleware
app.use(cors());
app.use(express.json());

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: async (req, file, cb) => {
    const uploadDir = path.join(__dirname, 'uploads');
    try {
      await fs.mkdir(uploadDir, { recursive: true });
      cb(null, uploadDir);
    } catch (error) {
      cb(error);
    }
  },
  filename: (req, file, cb) => {
    // Keep original filename with timestamp prefix
    cb(null, Date.now() + '-' + file.originalname);
  }
});

const upload = multer({ storage: storage });

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'OK', gdal_version: gdal.version });
});

// File upload and processing endpoint
app.post('/process-geospatial', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const filePath = req.file.path;
    const operation = req.body.operation || 'info';
    const originalName = req.file.originalname.toLowerCase();

    // For ZIP files, use GDAL's /vsizip/ virtual file system
    let gdalPath = filePath;
    if (originalName.endsWith('.zip')) {
      gdalPath = `/vsizip/${filePath}`;
    }

    let result;

    switch (operation) {
      case 'info':
        result = await getFileInfo(gdalPath);
        break;
      case 'detailed-info':
        result = await getDetailedInfo(gdalPath);
        break;
      case 'list-layers':
        result = await listAllLayers(gdalPath);
        break;
      case 'convert':
        const format = req.body.format || 'GeoJSON';
        result = await convertFile(gdalPath, format);
        break;
      default:
        result = await getFileInfo(gdalPath);
    }

    // Clean up uploaded file
    await fs.unlink(filePath);

    res.json(result);

  } catch (error) {
    console.error('Processing error:', error);
    
    // Clean up file if it exists
    if (req.file?.path) {
      try {
        await fs.unlink(req.file.path);
      } catch (cleanupError) {
        console.error('Cleanup error:', cleanupError);
      }
    }

    res.status(500).json({ 
      error: 'Processing failed', 
      details: error.message 
    });
  }
});

// GDAL processing functions
async function getFileInfo(filePath) {
  const result = await gdal.openAsync(filePath);
  
  // Check the correct structure in the async result
  let layerCount = 0;
  if (result.datasets && result.datasets.length > 0) {
    const firstDataset = result.datasets[0];
    if (firstDataset.layers && Array.isArray(firstDataset.layers)) {
      layerCount = firstDataset.layers.length;
    }
  }
  
  const info = {
    driver: result.driver?.description || 'Unknown',
    size: {
      width: result.rasterSize?.x || 0,
      height: result.rasterSize?.y || 0
    },
    layers: layerCount,
    projection: result.srs?.toWKT() || 'Unknown',
    extent: null
  };

  return info;
}

// Enhanced function to get detailed metadata
async function getDetailedInfo(gdalPath) {
  try {
    console.log(`Opening dataset: ${gdalPath}`);
    const result = await gdal.openAsync(gdalPath);
    
    // Comprehensive logging of the result structure
    console.log('=== GDAL OPEN RESULT ANALYSIS ===');
    console.log('Type of result:', typeof result);
    console.log('Result constructor:', result.constructor?.name);
    console.log('All result keys:', Object.keys(result));
    console.log('All result properties:', Object.getOwnPropertyNames(result));
    
    // Check for different possible structures
    console.log('Direct properties check:');
    console.log('- result.datasets:', !!result.datasets, Array.isArray(result.datasets), result.datasets?.length);
    console.log('- result.layers:', !!result.layers, typeof result.layers);
    console.log('- result.rootGroup:', !!result.rootGroup, typeof result.rootGroup);
    console.log('- result.driver:', !!result.driver, result.driver?.description);
    
    // Try to access layers through different paths
    if (result.layers) {
      console.log('Direct layers found:', typeof result.layers);
      if (typeof result.layers.count === 'function') {
        console.log('Layers count method available:', result.layers.count());
      }
      if (typeof result.layers.get === 'function') {
        console.log('Layers get method available');
      }
      if (Array.isArray(result.layers)) {
        console.log('Layers is array with length:', result.layers.length);
      }
    }
    
    if (result.rootGroup) {
      console.log('RootGroup found:', Object.keys(result.rootGroup));
      if (result.rootGroup.layers) {
        console.log('RootGroup.layers:', typeof result.rootGroup.layers);
        if (result.rootGroup.layers.names) {
          console.log('RootGroup.layers.names:', result.rootGroup.layers.names);
        }
      }
    }
    
    // Log the first few properties of result to see what we're working with
    for (const key of Object.keys(result).slice(0, 10)) {
      const value = result[key];
      console.log(`result.${key}:`, typeof value, Array.isArray(value) ? `Array(${value.length})` : value);
    }
    
    console.log('=== END ANALYSIS ===');

    const info = {
      file_info: {
        driver: result.driver?.description || 'Unknown',
        file_path: gdalPath,
        layer_count: 0,
        type: 'vector'
      },
      layers: [],
      debug_info: {
        result_type: typeof result,
        result_keys: Object.keys(result),
        has_datasets: !!result.datasets,
        has_layers: !!result.layers,
        has_rootGroup: !!result.rootGroup
      }
    };

    // Try all possible ways to access layers
    let layersFound = false;
    
    // Method 1: Direct datasets array
    if (result.datasets && result.datasets.length > 0) {
      console.log('Trying method 1: result.datasets[0].layers');
      const firstDataset = result.datasets[0];
      console.log('First dataset keys:', Object.keys(firstDataset));
      
      if (firstDataset.layers && Array.isArray(firstDataset.layers)) {
        console.log('Found layers in datasets[0].layers');
        info.layers = firstDataset.layers.map((layer, i) => ({
          name: layer.name || `Layer_${i}`,
          feature_count: layer.featureCount || 0,
          raw_data: layer
        }));
        info.file_info.layer_count = info.layers.length;
        layersFound = true;
      }
    }
    
    // Method 2: Direct layers property with count()
    if (!layersFound && result.layers && typeof result.layers.count === 'function') {
      console.log('Trying method 2: result.layers.count()');
      const layerCount = result.layers.count();
      info.file_info.layer_count = layerCount;
      
      for (let i = 0; i < layerCount; i++) {
        try {
          const layer = result.layers.get(i);
          info.layers.push({
            name: layer.name || `Layer_${i}`,
            feature_count: layer.features ? layer.features.count() : 0,
            raw_data: layer
          });
        } catch (e) {
          console.error(`Error getting layer ${i}:`, e.message);
        }
      }
      layersFound = true;
    }
    
    // Method 3: rootGroup.layers
    if (!layersFound && result.rootGroup && result.rootGroup.layers) {
      console.log('Trying method 3: result.rootGroup.layers');
      const rootLayers = result.rootGroup.layers;
      if (rootLayers.names && Array.isArray(rootLayers.names)) {
        info.file_info.layer_count = rootLayers.names.length;
        for (const layerName of rootLayers.names) {
          try {
            const layer = rootLayers.get(layerName);
            info.layers.push({
              name: layerName,
              feature_count: layer.features ? layer.features.count() : 0,
              raw_data: layer
            });
          } catch (e) {
            console.error(`Error getting layer ${layerName}:`, e.message);
          }
        }
        layersFound = true;
      }
    }

    if (!layersFound) {
      console.log('No layers found through any method. Full result object:');
      console.log(JSON.stringify(result, null, 2));
    }

    console.log(`Final result: found ${info.layers.length} layers`);
    return info;
    
  } catch (error) {
    console.error('Detailed info error:', error);
    throw new Error(`Failed to get detailed info: ${error.message}`);
  }
}

// Function to list all layers
async function listAllLayers(gdalPath) {
  try {
    const result = await gdal.openAsync(gdalPath);
    
    let layerCount = 0;
    const allLayers = [];
    
    // Use the correct structure from async result
    if (result.datasets && result.datasets.length > 0) {
      const firstDataset = result.datasets[0];
      
      if (firstDataset.layers && Array.isArray(firstDataset.layers)) {
        const layers = firstDataset.layers;
        layerCount = layers.length;
        
        for (let i = 0; i < layers.length; i++) {
          const layerData = layers[i];
          let geometryType = 'Unknown';
          
          // Extract geometry type from geometryFields
          if (layerData.geometryFields && Array.isArray(layerData.geometryFields) && layerData.geometryFields.length > 0) {
            geometryType = layerData.geometryFields[0].type || 'Unknown';
          }
          
          allLayers.push({
            index: i,
            name: layerData.name || `Layer_${i}`,
            geometry_type: geometryType,
            feature_count: layerData.featureCount || 0
          });
        }
      }
    }
    
    const resultInfo = {
      file_info: {
        driver: result.driver?.description || 'Unknown',
        layer_count: layerCount,
        file_path: gdalPath
      },
      layers: allLayers
    };

    return resultInfo;
    
  } catch (error) {
    throw new Error(`Failed to list layers: ${error.message}`);
  }
}

async function convertFile(inputPath, outputFormat) {
  const outputPath = inputPath.replace(path.extname(inputPath), '.converted');
  
  const dataset = await gdal.openAsync(inputPath);
  
  let driver;
  let extension;
  
  switch (outputFormat.toUpperCase()) {
    case 'GEOJSON':
      driver = gdal.drivers.get('GeoJSON');
      extension = '.geojson';
      break;
    case 'SHAPEFILE':
      driver = gdal.drivers.get('ESRI Shapefile');
      extension = '.shp';
      break;
    case 'KML':
      driver = gdal.drivers.get('KML');
      extension = '.kml';
      break;
    default:
      throw new Error(`Unsupported format: ${outputFormat}`);
  }

  const finalOutputPath = outputPath + extension;
  const outputDataset = await driver.createAsync(finalOutputPath);

  // Copy layers
  for (let i = 0; i < dataset.layers.count(); i++) {
    const sourceLayer = dataset.layers.get(i);
    const outputLayer = await outputDataset.layers.createAsync(
      sourceLayer.name,
      sourceLayer.srs,
      sourceLayer.geomType
    );

    // Copy field definitions
    const fieldDefns = sourceLayer.fields.getNames().map(name => 
      sourceLayer.fields.get(name)
    );
    
    for (const fieldDefn of fieldDefns) {
      await outputLayer.fields.addAsync(fieldDefn);
    }

    // Copy features
    await sourceLayer.features.forEachAsync(async (feature) => {
      await outputLayer.features.addAsync(feature);
    });
  }

  await outputDataset.flushAsync();
  outputDataset.close();
  dataset.close();

  // Read the converted file and return as string/object
  const convertedData = await fs.readFile(finalOutputPath, 'utf-8');
  
  // Clean up
  await fs.unlink(finalOutputPath);

  return {
    format: outputFormat,
    data: outputFormat.toUpperCase() === 'GEOJSON' ? JSON.parse(convertedData) : convertedData
  };
}

app.listen(port, '0.0.0.0', () => {
  console.log(`GDAL server running on port ${port}`);
});