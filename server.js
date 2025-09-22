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
  
  const info = {
    driver: dataset.driver.description,
    size: {
      width: dataset.rasterSize?.x || 0,
      height: dataset.rasterSize?.y || 0
    },
    layers: dataset.layers?.count() || 0,
    projection: dataset.srs?.toWKT() || 'Unknown',
    extent: null
  };

  // Get extent if it's a vector file
  if (dataset.layers?.count() > 0) {
    const layer = dataset.layers.get(0);
    const extent = layer.extent;
    if (extent) {
      info.extent = {
        minX: extent.minX,
        minY: extent.minY,
        maxX: extent.maxX,
        maxY: extent.maxY
      };
    }
  }

  dataset.close();
  return info;
}

// Enhanced function to get detailed metadata
async function getDetailedInfo(gdalPath) {
  try {
    const dataset = await gdal.openAsync(gdalPath);
    
    const info = {
      file_info: {
        driver: dataset.driver.description,
        file_path: gdalPath,
        layer_count: dataset.layers?.count() || 0
      },
      layers: []
    };

    // If it's a raster
    if (dataset.rasterSize) {
      info.file_info.type = 'raster';
      info.file_info.size = {
        width: dataset.rasterSize.x,
        height: dataset.rasterSize.y
      };
      info.file_info.band_count = dataset.bands?.count() || 0;
    } else {
      info.file_info.type = 'vector';
    }

    // Process each layer
    if (dataset.layers && dataset.layers.count() > 0) {
      for (let i = 0; i < dataset.layers.count(); i++) {
        const layer = dataset.layers.get(i);
        
        const layerInfo = {
          name: layer.name,
          geometry_type: layer.geomType ? gdal.wkbGeometryType[layer.geomType] : 'Unknown',
          feature_count: layer.features.count(),
          spatial_reference: layer.srs ? layer.srs.toWKT() : 'Unknown',
          extent: null,
          fields: []
        };

        // Get layer extent
        try {
          const extent = layer.extent;
          if (extent) {
            layerInfo.extent = {
              min_x: extent.minX,
              min_y: extent.minY,
              max_x: extent.maxX,
              max_y: extent.maxY
            };
          }
        } catch (extentError) {
          console.warn('Could not get extent for layer:', layer.name);
        }

        // Get field definitions
        if (layer.fields) {
          const fieldNames = layer.fields.getNames();
          for (const fieldName of fieldNames) {
            try {
              const field = layer.fields.get(fieldName);
              layerInfo.fields.push({
                name: fieldName,
                type: gdal.fieldType[field.type] || 'Unknown',
                width: field.width || null,
                precision: field.precision || null,
                justification: field.justification ? gdal.fieldJustification[field.justification] : null
              });
            } catch (fieldError) {
              console.warn(`Could not get field info for ${fieldName}:`, fieldError.message);
            }
          }
        }

        // Get a sample of features to show actual data structure
        layerInfo.sample_features = [];
        if (layer.features.count() > 0) {
          let sampleCount = 0;
          const maxSamples = 3;
          
          await layer.features.forEachAsync(async (feature) => {
            if (sampleCount >= maxSamples) return;
            
            const featureData = {
              fid: feature.fid,
              geometry_type: feature.getGeometry() ? 
                gdal.wkbGeometryType[feature.getGeometry().wkbType] : null,
              properties: {}
            };

            // Get feature properties
            const fieldNames = layer.fields.getNames();
            for (const fieldName of fieldNames) {
              try {
                featureData.properties[fieldName] = feature.fields.get(fieldName);
              } catch (err) {
                featureData.properties[fieldName] = null;
              }
            }

            layerInfo.sample_features.push(featureData);
            sampleCount++;
          });
        }

        info.layers.push(layerInfo);
      }
    }

    dataset.close();
    return info;
    
  } catch (error) {
    throw new Error(`Failed to get detailed info: ${error.message}`);
  }
}

// Function to list all layers
async function listAllLayers(gdalPath) {
  try {
    const dataset = await gdal.openAsync(gdalPath);
    
    const result = {
      file_info: {
        driver: dataset.driver.description,
        layer_count: dataset.layers?.count() || 0,
        file_path: gdalPath
      },
      layers: []
    };

    if (dataset.layers && dataset.layers.count() > 0) {
      for (let i = 0; i < dataset.layers.count(); i++) {
        const layer = dataset.layers.get(i);
        result.layers.push({
          index: i,
          name: layer.name,
          geometry_type: layer.geomType ? gdal.wkbGeometryType[layer.geomType] : 'Unknown',
          feature_count: layer.features.count()
        });
      }
    }

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