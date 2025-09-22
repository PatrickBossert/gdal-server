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
  const dataset = await gdal.openAsync(filePath);
  
  // Check if this is the structure with datasets array
  let layerCount = 0;
  if (dataset.datasets && dataset.datasets.length > 0) {
    const firstDataset = dataset.datasets[0];
    if (firstDataset.info && firstDataset.info.layers) {
      layerCount = firstDataset.info.layers.length;
    }
  }
  
  const info = {
    driver: dataset.driver?.description || 'Unknown',
    size: {
      width: dataset.rasterSize?.x || 0,
      height: dataset.rasterSize?.y || 0
    },
    layers: layerCount,
    projection: dataset.srs?.toWKT() || 'Unknown',
    extent: null
  };

  dataset.close();
  return info;
}

// Enhanced function to get detailed metadata
async function getDetailedInfo(gdalPath) {
  let dataset;
  try {
    console.log(`Opening dataset: ${gdalPath}`);
    dataset = await gdal.openAsync(gdalPath);
    
    // Log the structure we receive
    console.log('Dataset structure:', {
      hasDatasets: !!dataset.datasets,
      datasetsLength: dataset.datasets?.length || 0,
      driver: dataset.driver?.description
    });

    const info = {
      file_info: {
        driver: dataset.driver?.description || 'Unknown',
        file_path: gdalPath,
        layer_count: 0,
        type: 'vector'
      },
      layers: []
    };

    // Access layers through datasets[0].info.layers structure
    if (dataset.datasets && dataset.datasets.length > 0) {
      const firstDataset = dataset.datasets[0];
      console.log('First dataset info:', {
        hasInfo: !!firstDataset.info,
        hasLayers: !!(firstDataset.info && firstDataset.info.layers),
        layerCount: firstDataset.info?.layers?.length || 0
      });

      if (firstDataset.info && firstDataset.info.layers) {
        const layers = firstDataset.info.layers;
        info.file_info.layer_count = layers.length;
        
        console.log(`Found ${layers.length} layers`);

        // Process each layer
        for (let i = 0; i < layers.length; i++) {
          try {
            console.log(`Processing layer ${i + 1}/${layers.length}`);
            const layerInfo = layers[i];
            
            const processedLayer = {
              name: layerInfo.name || `Layer_${i}`,
              geometry_type: layerInfo.geometryType || 'Unknown',
              feature_count: layerInfo.featureCount || 0,
              spatial_reference: 'Unknown',
              extent: null,
              fields: []
            };

            // Extract field information if available
            if (layerInfo.fields) {
              console.log(`Layer ${processedLayer.name} has ${layerInfo.fields.length} fields`);
              
              for (const field of layerInfo.fields) {
                processedLayer.fields.push({
                  name: field.name || 'Unknown',
                  type: field.type || 'Unknown',
                  width: field.width || null,
                  precision: field.precision || null,
                  justification: field.justification || null
                });
              }
            }

            // Add extent information if available
            if (layerInfo.extent) {
              processedLayer.extent = {
                min_x: layerInfo.extent.minX,
                min_y: layerInfo.extent.minY,
                max_x: layerInfo.extent.maxX,
                max_y: layerInfo.extent.maxY
              };
            }

            // Add sample features placeholder (since we can't access actual features from info structure)
            processedLayer.sample_features = [];
            processedLayer.note = "Feature sampling not available from info structure";

            info.layers.push(processedLayer);
            console.log(`Successfully processed layer: ${processedLayer.name}`);
            
          } catch (layerError) {
            console.error(`Error processing layer ${i}:`, layerError.message);
            info.layers.push({
              name: `Layer_${i}_Error`,
              error: layerError.message,
              geometry_type: 'Error',
              feature_count: 0,
              spatial_reference: 'Error',
              extent: null,
              fields: []
            });
          }
        }
      }
    } else {
      console.log('No datasets array found, trying alternative access methods...');
      // Fallback: try other possible structures
      // We can add more structure checks here if needed
    }

    dataset.close();
    console.log(`Successfully processed ${info.layers.length} layers`);
    return info;
    
  } catch (error) {
    if (dataset) {
      try {
        dataset.close();
      } catch (closeError) {
        console.error('Error closing dataset:', closeError.message);
      }
    }
    console.error('Detailed info error:', error);
    throw new Error(`Failed to get detailed info: ${error.message}`);
  }
}

// Function to list all layers
async function listAllLayers(gdalPath) {
  try {
    const dataset = await gdal.openAsync(gdalPath);
    
    let layerCount = 0;
    const allLayers = [];
    
    // Access layers through datasets[0].info.layers structure
    if (dataset.datasets && dataset.datasets.length > 0) {
      const firstDataset = dataset.datasets[0];
      if (firstDataset.info && firstDataset.info.layers) {
        const layers = firstDataset.info.layers;
        layerCount = layers.length;
        
        for (let i = 0; i < layers.length; i++) {
          const layerInfo = layers[i];
          allLayers.push({
            index: i,
            name: layerInfo.name || `Layer_${i}`,
            geometry_type: layerInfo.geometryType || 'Unknown',
            feature_count: layerInfo.featureCount || 0
          });
        }
      }
    }
    
    const result = {
      file_info: {
        driver: dataset.driver?.description || 'Unknown',
        layer_count: layerCount,
        file_path: gdalPath
      },
      layers: allLayers
    };

    dataset.close();
    return result;
    
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