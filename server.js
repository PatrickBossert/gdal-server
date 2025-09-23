const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 8000;

// Middleware
app.use(cors());
app.use(express.json());

// File upload configuration
const upload = multer({
  dest: '/tmp/',
  limits: { fileSize: 100 * 1024 * 1024 }, // 100MB
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK',
    gdal_version: '3.11.4',
    timestamp: new Date().toISOString()
  });
});

// Function to format file path for GDAL
async function formatGDALPath(filePath, originalName) {
  // If it's a ZIP file, use GDAL's /vsizip/ virtual file system
  if (originalName.toLowerCase().endsWith('.zip')) {
    const gdal = require('gdal-async');
    
    try {
      // First, try to list contents of the ZIP to find the .gdb directory
      const zipPath = `/vsizip/${filePath}`;
      const dataset = await gdal.openAsync(zipPath);
      
      // If this works, return the zip path directly
      return zipPath;
      
    } catch (error) {
      // If direct access fails, we need to find the .gdb directory inside
      // For now, assume the .gdb has the same name as the ZIP file
      const baseName = path.basename(originalName, '.zip');
      const gdbPath = `/vsizip/${filePath}/${baseName}.gdb`;
      
      console.log(`Trying GDB path: ${gdbPath}`);
      return gdbPath;
    }
  }
  return filePath;
}

// Basic file info function
async function getFileInfo(filePath) {
  const gdal = require('gdal-async');
  
  try {
    const dataset = await gdal.openAsync(filePath);
    const layerCount = dataset.layers.count();
    
    return {
      file_info: {
        type: 'Geospatial',
        driver: dataset.description || 'Unknown',
        layer_count: layerCount
      }
    };
  } catch (error) {
    throw new Error(`Failed to read file: ${error.message}`);
  }
}

// Detailed info function
async function getDetailedInfo(filePath) {
  const gdal = require('gdal-async');
  
  try {
    const dataset = await gdal.openAsync(filePath);
    const layers = [];
    
    dataset.layers.forEach((layer) => {
      const fields = layer.fields.toArray().map(field => ({
        name: field.name,
        type: field.type,
        width: field.width
      }));
      
      layers.push({
        name: layer.name,
        geometry_type: layer.geomType,
        feature_count: layer.features.count(),
        fields: fields
      });
    });
    
    return {
      file_info: {
        type: 'Geospatial',
        driver: dataset.description || 'Unknown',
        layer_count: dataset.layers.count()
      },
      layers: layers
    };
  } catch (error) {
    throw new Error(`Failed to get detailed info: ${error.message}`);
  }
}

// Simple layer extraction (no coordinate transformation to avoid bugs)
async function extractLayerBasic(filePath, layerName) {
  const gdal = require('gdal-async');
  
  try {
    const dataset = await gdal.openAsync(filePath);
    const layer = dataset.layers.get(layerName);
    
    if (!layer) {
      throw new Error(`Layer "${layerName}" not found`);
    }
    
    const features = [];
    
    layer.features.forEach((feature) => {
      const geometry = feature.getGeometry();
      const geoJSON = geometry ? geometry.toObject() : null;
      
      features.push({
        fid: feature.fid,
        geometry: geoJSON,
        properties: feature.fields.toObject()
      });
    });
    
    return {
      name: layerName,
      geometry_type: features.length > 0 ? features[0].geometry?.type : 'Unknown',
      feature_count: features.length,
      coordinate_transformation_applied: false,
      features: features
    };
  } catch (error) {
    throw new Error(`Failed to extract layer: ${error.message}`);
  }
}

// Main processing endpoint
app.post('/process-geospatial', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }
    
    console.log(`Processing file: ${req.file.originalname}`);
    
    // Use GDAL's native ZIP handling with /vsizip/
    const gdalPath = await formatGDALPath(req.file.path, req.file.originalname);
    console.log(`GDAL path: ${gdalPath}`);
    
    const operation = req.body.operation;
    let result;
    
    switch (operation) {
      case 'info':
        result = await getFileInfo(gdalPath);
        break;
        
      case 'detailed-info':
        result = await getDetailedInfo(gdalPath);
        break;
        
      case 'list-layers':
        result = await getFileInfo(gdalPath);
        break;
        
      case 'extract-layer':
        const layerName = req.body.layerName;
        if (!layerName) {
          return res.status(400).json({ error: 'Layer name required' });
        }
        result = await extractLayerBasic(gdalPath, layerName);
        break;
        
      default:
        return res.status(400).json({ error: 'Unknown operation' });
    }
    
    // Clean up uploaded file
    fs.unlinkSync(req.file.path);
    
    res.json(result);
    
  } catch (error) {
    console.error('Processing error:', error);
    
    // Clean up file on error
    if (req.file) {
      try {
        fs.unlinkSync(req.file.path);
      } catch (cleanupError) {
        console.error('Cleanup error:', cleanupError);
      }
    }
    
    res.status(500).json({ error: error.message });
  }
});

// Start server with proper Railway configuration
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
  console.log('Ready to accept connections');
});
