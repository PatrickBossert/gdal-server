const express = require('express');
const multer = require('multer');
const cors = require('cors');
const gdal = require('gdal-async');
const fs = require('fs').promises;
const path = require('path');

const app = express();
const port = parseInt(process.env.PORT) || 3001;

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

    let result;

    switch (operation) {
      case 'info':
        result = await getFileInfo(filePath);
        break;
      case 'convert':
        const format = req.body.format || 'GeoJSON';
        result = await convertFile(filePath, format);
        break;
      case 'reproject':
        const targetSRS = req.body.targetSRS || 'EPSG:4326';
        result = await reprojectFile(filePath, targetSRS);
        break;
      default:
        result = await getFileInfo(filePath);
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

async function reprojectFile(inputPath, targetSRS) {
  const dataset = await gdal.openAsync(inputPath);
  const outputPath = inputPath.replace(path.extname(inputPath), '_reprojected.geojson');
  
  const driver = gdal.drivers.get('GeoJSON');
  const outputDataset = await driver.createAsync(outputPath);

  const targetSpatialRef = gdal.SpatialReference.fromUserInput(targetSRS);

  for (let i = 0; i < dataset.layers.count(); i++) {
    const sourceLayer = dataset.layers.get(i);
    const outputLayer = await outputDataset.layers.createAsync(
      sourceLayer.name,
      targetSpatialRef,
      sourceLayer.geomType
    );

    // Copy field definitions
    const fieldDefns = sourceLayer.fields.getNames().map(name => 
      sourceLayer.fields.get(name)
    );
    
    for (const fieldDefn of fieldDefns) {
      await outputLayer.fields.addAsync(fieldDefn);
    }

    // Transform and copy features
    const transformation = new gdal.CoordinateTransformation(sourceLayer.srs, targetSpatialRef);
    
    await sourceLayer.features.forEachAsync(async (feature) => {
      const geom = feature.getGeometry();
      if (geom) {
        geom.transform(transformation);
        feature.setGeometry(geom);
      }
      await outputLayer.features.addAsync(feature);
    });
  }

  await outputDataset.flushAsync();
  outputDataset.close();
  dataset.close();

  const reprojectedData = await fs.readFile(outputPath, 'utf-8');
  await fs.unlink(outputPath);

  return {
    targetSRS,
    data: JSON.parse(reprojectedData)
  };
}

app.listen(port, () => {
  console.log(`GDAL server running on port ${port}`);
  console.log(`GDAL version: ${gdal.version}`);
});