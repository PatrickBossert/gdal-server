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
      case 'extract-layer':
        const layerName = req.body.layerName;
        const transformCoords = req.body.transformCoords !== 'false'; // Default to true
        if (!layerName) {
          return res.status(400).json({ error: 'Layer name is required for extraction' });
        }
        result = await extractLayerFeatures(gdalPath, layerName, transformCoords);
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
  
  // Use the working method: result.layers.count()
  let layerCount = 0;
  if (result.layers && typeof result.layers.count === 'function') {
    layerCount = result.layers.count();
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

    const info = {
      file_info: {
        driver: result.driver?.description || 'Unknown',
        file_path: gdalPath,
        layer_count: 0,
        type: 'vector'
      },
      layers: []
    };

    // Use the working method: result.layers.count()
    if (result.layers && typeof result.layers.count === 'function') {
      console.log('Using direct layers access method');
      const layerCount = result.layers.count();
      info.file_info.layer_count = layerCount;
      
      console.log(`Found ${layerCount} layers`);

      for (let i = 0; i < layerCount; i++) {
        try {
          console.log(`Processing layer ${i + 1}/${layerCount}`);
          const layer = result.layers.get(i);
          
          const layerInfo = {
            name: layer.name || `Layer_${i}`,
            geometry_type: 'Unknown',
            feature_count: 0,
            spatial_reference: 'Unknown',
            extent: null,
            fields: []
          };

          // Get geometry type
          try {
            if (layer.geomType) {
              layerInfo.geometry_type = gdal.wkbGeometryType[layer.geomType] || `Type_${layer.geomType}`;
            }
          } catch (geomError) {
            console.warn(`Could not get geometry type for layer ${layer.name}:`, geomError.message);
          }

          // Get feature count
          try {
            if (layer.features && typeof layer.features.count === 'function') {
              layerInfo.feature_count = layer.features.count();
            }
          } catch (countError) {
            console.warn(`Could not get feature count for layer ${layer.name}:`, countError.message);
          }

          // Get spatial reference
          try {
            if (layer.srs) {
              layerInfo.spatial_reference = layer.srs.toWKT();
            }
          } catch (srsError) {
            console.warn(`Could not get SRS for layer ${layer.name}:`, srsError.message);
          }

          // Get extent
          try {
            if (layer.extent) {
              layerInfo.extent = {
                min_x: layer.extent.minX,
                min_y: layer.extent.minY,
                max_x: layer.extent.maxX,
                max_y: layer.extent.maxY
              };
            }
          } catch (extentError) {
            console.warn(`Could not get extent for layer ${layer.name}:`, extentError.message);
          }

          // Get field definitions
          try {
            if (layer.fields) {
              const fieldNames = layer.fields.getNames();
              console.log(`Layer ${layer.name} has ${fieldNames.length} fields`);
              
              for (const fieldName of fieldNames) {
                try {
                  const field = layer.fields.get(fieldName);
                  layerInfo.fields.push({
                    name: fieldName,
                    type: gdal.fieldType[field.type] || `Type_${field.type}`,
                    width: field.width || null,
                    precision: field.precision || null,
                    nullable: field.nullable !== undefined ? field.nullable : null,
                    justification: field.justification ? gdal.fieldJustification[field.justification] : null
                  });
                } catch (fieldError) {
                  console.warn(`Could not get field info for ${fieldName}:`, fieldError.message);
                  layerInfo.fields.push({
                    name: fieldName,
                    type: 'Error',
                    width: null,
                    precision: null,
                    nullable: null
                  });
                }
              }
            }
          } catch (fieldsError) {
            console.warn(`Could not get fields for layer ${layer.name}:`, fieldsError.message);
          }

          // Get sample features (limit to 1 to avoid timeouts)
          layerInfo.sample_features = [];
          try {
            if (layerInfo.feature_count > 0 && layer.features) {
              let sampleCount = 0;
              const maxSamples = 1;
              
              await layer.features.forEachAsync(async (feature) => {
                if (sampleCount >= maxSamples) return;
                
                try {
                  const featureData = {
                    fid: feature.fid || null,
                    geometry_type: null,
                    properties: {}
                  };

                  // Get geometry type for this feature
                  try {
                    const geom = feature.getGeometry();
                    if (geom) {
                      featureData.geometry_type = gdal.wkbGeometryType[geom.wkbType] || `Type_${geom.wkbType}`;
                    }
                  } catch (geomError) {
                    console.warn(`Could not get geometry for feature ${feature.fid}`);
                  }

                  // Get feature properties (limit to first 5 fields)
                  const fieldNames = layer.fields?.getNames() || [];
                  const limitedFields = fieldNames.slice(0, 5);
                  
                  for (const fieldName of limitedFields) {
                    try {
                      featureData.properties[fieldName] = feature.fields.get(fieldName);
                    } catch (propError) {
                      featureData.properties[fieldName] = null;
                    }
                  }

                  layerInfo.sample_features.push(featureData);
                  sampleCount++;
                } catch (featureError) {
                  console.warn(`Error processing sample feature in layer ${layer.name}:`, featureError.message);
                }
              });
            }
          } catch (sampleError) {
            console.warn(`Could not get sample features for layer ${layer.name}:`, sampleError.message);
            layerInfo.note = `Sample feature extraction failed: ${sampleError.message}`;
          }

          info.layers.push(layerInfo);
          console.log(`Successfully processed layer: ${layerInfo.name} (${layerInfo.feature_count} features, ${layerInfo.fields.length} fields)`);
          
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
    } else {
      console.log('No layers found with count() method');
      return {
        ...info,
        error: 'Could not access layers - unsupported dataset structure'
      };
    }

    console.log(`Successfully processed ${info.layers.length} layers`);
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
    
    // Use the working method: result.layers.count()
    if (result.layers && typeof result.layers.count === 'function') {
      layerCount = result.layers.count();
      
      for (let i = 0; i < layerCount; i++) {
        try {
          const layer = result.layers.get(i);
          let geometryType = 'Unknown';
          let featureCount = 0;
          
          // Get geometry type
          if (layer.geomType) {
            geometryType = gdal.wkbGeometryType[layer.geomType] || `Type_${layer.geomType}`;
          }
          
          // Get feature count
          if (layer.features && typeof layer.features.count === 'function') {
            featureCount = layer.features.count();
          }
          
          allLayers.push({
            index: i,
            name: layer.name || `Layer_${i}`,
            geometry_type: geometryType,
            feature_count: featureCount
          });
        } catch (layerError) {
          console.warn(`Could not get layer ${i}:`, layerError.message);
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

// Extract complete layer with coordinate transformation
async function extractLayerFeatures(gdalPath, layerName, transformCoords = true) {
  try {
    console.log(`Extracting layer "${layerName}" from: ${gdalPath}`);
    const result = await gdal.openAsync(gdalPath);
    
    // Find the specified layer
    if (!result.layers || typeof result.layers.count !== 'function') {
      throw new Error('No layers found in dataset');
    }
    
    let targetLayer = null;
    const layerCount = result.layers.count();
    
    for (let i = 0; i < layerCount; i++) {
      const layer = result.layers.get(i);
      if (layer.name === layerName) {
        targetLayer = layer;
        break;
      }
    }
    
    if (!targetLayer) {
      throw new Error(`Layer "${layerName}" not found. Available layers: ${Array.from({length: layerCount}, (_, i) => result.layers.get(i).name).join(', ')}`);
    }
    
    console.log(`Found layer "${layerName}" with ${targetLayer.features.count()} features`);
    
    // Set up coordinate transformation if requested
    let coordTransform = null;
    if (transformCoords && targetLayer.srs) {
      try {
        // Create target spatial reference (WGS84 - EPSG:4326)
        const targetSrs = gdal.SpatialReference.fromEPSG(4326);
        coordTransform = new gdal.CoordinateTransformation(targetLayer.srs, targetSrs);
        console.log(`Coordinate transformation enabled: ${targetLayer.srs.toProj4()} -> WGS84`);
      } catch (transformError) {
        console.warn('Could not set up coordinate transformation:', transformError.message);
        coordTransform = null;
      }
    }
    
    // Extract layer information
    const layerInfo = {
      name: layerName,
      feature_count: targetLayer.features.count(),
      geometry_type: targetLayer.geomType ? gdal.wkbGeometryType[targetLayer.geomType] : 'Unknown',
      spatial_reference: {
        original: targetLayer.srs ? targetLayer.srs.toWKT() : 'Unknown',
        transformed: transformCoords ? 'EPSG:4326 (WGS84)' : null
      },
      coordinate_transformation_applied: !!coordTransform,
      fields: [],
      features: []
    };
    
    // Get field definitions
    if (targetLayer.fields) {
      const fieldNames = targetLayer.fields.getNames();
      for (const fieldName of fieldNames) {
        try {
          const field = targetLayer.fields.get(fieldName);
          layerInfo.fields.push({
            name: fieldName,
            type: gdal.fieldType[field.type] || 'Unknown',
            width: field.width || null,
            precision: field.precision || null
          });
        } catch (fieldError) {
          console.warn(`Could not get field info for ${fieldName}:`, fieldError.message);
        }
      }
    }
    
    // Extract all features
    console.log('Starting feature extraction...');
    let featureCount = 0;
    const maxFeatures = 10000; // Limit to prevent memory issues
    
    await targetLayer.features.forEachAsync(async (feature) => {
      if (featureCount >= maxFeatures) {
        console.log(`Reached maximum feature limit (${maxFeatures}), stopping extraction`);
        return;
      }
      
      try {
        const featureData = {
          fid: feature.fid,
          geometry: null,
          properties: {}
        };
        
        // Extract geometry
        const geometry = feature.getGeometry();
        if (geometry) {
          // Apply coordinate transformation if configured
          if (coordTransform) {
            try {
              const transformedGeometry = geometry.clone();
              transformedGeometry.transform(coordTransform);
              featureData.geometry = JSON.parse(transformedGeometry.toJSON());
            } catch (transformError) {
              console.warn(`Failed to transform geometry for feature ${feature.fid}:`, transformError.message);
              // Fallback to original geometry
              featureData.geometry = JSON.parse(geometry.toJSON());
            }
          } else {
            featureData.geometry = JSON.parse(geometry.toJSON());
          }
        }
        
        // Extract properties
        const fieldNames = targetLayer.fields?.getNames() || [];
        for (const fieldName of fieldNames) {
          try {
            featureData.properties[fieldName] = feature.fields.get(fieldName);
          } catch (propError) {
            featureData.properties[fieldName] = null;
          }
        }
        
        layerInfo.features.push(featureData);
        featureCount++;
        
        // Log progress every 100 features
        if (featureCount % 100 === 0) {
          console.log(`Extracted ${featureCount} features...`);
        }
        
      } catch (featureError) {
        console.warn(`Error processing feature ${feature.fid}:`, featureError.message);
      }
    });
    
    console.log(`Successfully extracted ${featureCount} features from layer "${layerName}"`);
    
    // Update final count
    layerInfo.feature_count = featureCount;
    layerInfo.extraction_summary = {
      total_extracted: featureCount,
      coordinate_transformation_applied: !!coordTransform,
      max_feature_limit: maxFeatures,
      truncated: featureCount >= maxFeatures
    };
    
    return layerInfo;
    
  } catch (error) {
    console.error('Layer extraction error:', error);
    throw new Error(`Failed to extract layer: ${error.message}`);
  }
}
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