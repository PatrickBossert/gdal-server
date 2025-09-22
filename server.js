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
  
  // Check the correct structure: datasets[0].layers
  let layerCount = 0;
  if (dataset.datasets && dataset.datasets.length > 0) {
    const firstDataset = dataset.datasets[0];
    if (firstDataset.layers && Array.isArray(firstDataset.layers)) {
      layerCount = firstDataset.layers.length;
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
    
    console.log('Dataset structure check:', {
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

    // Use the correct structure: datasets[0].layers
    if (dataset.datasets && dataset.datasets.length > 0) {
      const firstDataset = dataset.datasets[0];
      
      if (firstDataset.layers && Array.isArray(firstDataset.layers)) {
        const layers = firstDataset.layers;
        info.file_info.layer_count = layers.length;
        
        console.log(`Found ${layers.length} layers in datasets[0].layers`);

        // Process each layer
        for (let i = 0; i < layers.length; i++) {
          try {
            console.log(`Processing layer ${i + 1}/${layers.length}`);
            const layerData = layers[i];
            
            const layerInfo = {
              name: layerData.name || `Layer_${i}`,
              feature_count: layerData.featureCount || 0,
              fid_column: layerData.fidColumnName || null,
              geometry_type: 'Unknown',
              spatial_reference: 'Unknown',
              extent: null,
              fields: []
            };

            // Extract geometry information
            if (layerData.geometryFields && Array.isArray(layerData.geometryFields) && layerData.geometryFields.length > 0) {
              const geomField = layerData.geometryFields[0];
              layerInfo.geometry_field_name = geomField.name || 'Shape';
              layerInfo.geometry_type = geomField.type || 'Unknown';
              
              // Extract spatial reference
              if (geomField.coordinateSystem) {
                layerInfo.spatial_reference = JSON.stringify(geomField.coordinateSystem);
              }
              
              // Extract extent
              if (geomField.extent && Array.isArray(geomField.extent) && geomField.extent.length >= 4) {
                layerInfo.extent = {
                  min_x: geomField.extent[0],
                  min_y: geomField.extent[1],
                  max_x: geomField.extent[2],
                  max_y: geomField.extent[3]
                };
              }
            }

            // Extract field information
            if (layerData.fields && Array.isArray(layerData.fields)) {
              console.log(`Layer ${layerInfo.name} has ${layerData.fields.length} fields`);
              
              for (const field of layerData.fields) {
                layerInfo.fields.push({
                  name: field.name || 'Unknown',
                  type: field.type || 'Unknown',
                  width: field.width || null,
                  nullable: field.nullable !== undefined ? field.nullable : null,
                  unique_constraint: field.uniqueConstraint !== undefined ? field.uniqueConstraint : null,
                  default_value: field.defaultValue || null,
                  alias: field.alias || null
                });
              }
            }

            // Note about feature sampling
            layerInfo.sample_features = [];
            layerInfo.note = "Feature data access requires direct layer queries - not available in metadata structure";

            info.layers.push(layerInfo);
            console.log(`Successfully processed layer: ${layerInfo.name} (${layerInfo.feature_count} features, ${layerInfo.fields.length} fields)`);
            
          } catch (layerError) {
            console.error(`Error processing layer ${i}:`, layerError.message);
            info.layers.push({
              name: `Layer_${i}_Error`,
              error: layerError.message,
              feature_count: 0,
              geometry_type: 'Error',
              fields: []
            });
          }
        }
      } else {
        console.log('datasets[0].layers not found or not an array');
      }
    } else {
      console.log('No datasets array found');
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
    
    // Use the correct structure: datasets[0].layers
    if (dataset.datasets && dataset.datasets.length > 0) {
      const firstDataset = dataset.datasets[0];
      
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