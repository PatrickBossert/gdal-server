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
function formatGDALPath(filePath, originalName) {
  // If it's a ZIP file, use GDAL's /vsizip/ virtual file system
  if (originalName.toLowerCase().endsWith('.zip')) {
    console.log(`Processing ZIP file: ${originalName}`);
    
    const zipPath = `/vsizip/${filePath}`;
    
    // Method 1: Try direct ZIP access first
    try {
      console.log(`Trying direct ZIP access: ${zipPath}`);
      const gdal = require('gdal-async');
      const dataset = gdal.open(zipPath);
      dataset.close();
      console.log(`Direct ZIP access successful: ${zipPath}`);
      return zipPath;
    } catch (error) {
      console.log(`Direct ZIP access failed: ${error.message}`);
    }
    
    // Method 2: Try with original case (UMN.GDB/)
    const baseNameOriginal = require('path').basename(originalName, '.zip');
    const originalCasePath = `/vsizip/${filePath}/${baseNameOriginal}`;
    
    try {
      console.log(`Trying original case path: ${originalCasePath}`);
      const gdal = require('gdal-async');
      const dataset = gdal.open(originalCasePath);
      dataset.close();
      console.log(`Original case path successful: ${originalCasePath}`);
      return originalCasePath;
    } catch (error) {
      console.log(`Original case path failed: ${error.message}`);
    }
    
    // Method 3: Try with .GDB extension (uppercase)
    let baseNameUpper = baseNameOriginal;
    if (!baseNameUpper.toUpperCase().endsWith('.GDB')) {
      baseNameUpper += '.GDB';
    }
    const upperCasePath = `/vsizip/${filePath}/${baseNameUpper}`;
    
    try {
      console.log(`Trying uppercase GDB path: ${upperCasePath}`);
      const gdal = require('gdal-async');
      const dataset = gdal.open(upperCasePath);
      dataset.close();
      console.log(`Uppercase GDB path successful: ${upperCasePath}`);
      return upperCasePath;
    } catch (error) {
      console.log(`Uppercase GDB path failed: ${error.message}`);
    }
    
    // Method 4: Try lowercase version
    let baseNameLower = baseNameOriginal.toLowerCase();
    if (!baseNameLower.endsWith('.gdb')) {
      baseNameLower += '.gdb';
    }
    const lowerCasePath = `/vsizip/${filePath}/${baseNameLower}`;
    
    try {
      console.log(`Trying lowercase gdb path: ${lowerCasePath}`);
      const gdal = require('gdal-async');
      const dataset = gdal.open(lowerCasePath);
      dataset.close();
      console.log(`Lowercase gdb path successful: ${lowerCasePath}`);
      return lowerCasePath;
    } catch (error) {
      console.log(`Lowercase gdb path failed: ${error.message}`);
    }
    
    // If all methods fail, return the most likely path (original case)
    console.log(`All methods failed, returning original case path: ${originalCasePath}`);
    return originalCasePath;
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
