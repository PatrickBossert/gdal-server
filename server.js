// Replace the extractLayerFeatures function in your server.js with this simplified version

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

async function extractLayerFeatures(inputPath, layerName, transformCoords = true) {
  try {
    console.log(`Extracting layer: ${layerName} from ${inputPath}`);
    console.log(`Transform coordinates: ${transformCoords}`);
    
    // Create temporary output path
    const tempDir = '/tmp';
    const outputPath = path.join(tempDir, `extracted_${Date.now()}.geojson`);
    
    // Build ogr2ogr command using GDAL's built-in transformation
    const cmd = [
      'ogr2ogr',
      '-f', 'GeoJSON',
      outputPath,
      inputPath,
      layerName
    ];
    
    // Add coordinate transformation if requested
    if (transformCoords) {
      cmd.push('-t_srs', 'EPSG:4326');
    }
    
    // Add additional options for better compatibility
    cmd.push('-lco', 'RFC7946=YES'); // Ensures proper GeoJSON format
    cmd.push('-skipfailures'); // Skip features with invalid geometry
    
    console.log('Executing command:', cmd.join(' '));
    
    // Execute the command
    const result = execSync(cmd.join(' '), { 
      encoding: 'utf8',
      maxBuffer: 50 * 1024 * 1024 // 50MB buffer for large datasets
    });
    
    // Read the generated GeoJSON
    if (!fs.existsSync(outputPath)) {
      throw new Error('Output file was not created');
    }
    
    const geojsonData = JSON.parse(fs.readFileSync(outputPath, 'utf8'));
    
    // Clean up temporary file
    fs.unlinkSync(outputPath);
    
    // Extract layer information
    const features = geojsonData.features || [];
    const layerInfo = {
      name: layerName,
      geometry_type: features.length > 0 ? features[0].geometry?.type : 'Unknown',
      feature_count: features.length,
      coordinate_transformation_applied: transformCoords,
      features: features.map((feature, index) => ({
        fid: index + 1,
        geometry_type: feature.geometry?.type,
        coordinates: feature.geometry?.coordinates,
        properties: feature.properties || {},
        geometry: feature.geometry
      }))
    };
    
    // Add spatial reference information
    if (transformCoords) {
      layerInfo.spatial_reference = {
        transformed: 'EPSG:4326 (WGS84)',
        original: 'Auto-detected from source'
      };
    }
    
    // Add extraction summary
    layerInfo.extraction_summary = {
      total_extracted: features.length,
      truncated: false,
      max_feature_limit: null
    };
    
    console.log(`Successfully extracted ${features.length} features from layer: ${layerName}`);
    return layerInfo;
    
  } catch (error) {
    console.error('Layer extraction error:', error.message);
    console.error('Error details:', error);
    
    // Provide helpful error information
    if (error.message.includes('Layer') && error.message.includes('not found')) {
      throw new Error(`Layer "${layerName}" not found in the dataset. Use "list-layers" or "detailed-info" to see available layers.`);
    } else if (error.message.includes('ogr2ogr')) {
      throw new Error(`GDAL processing error: ${error.message}`);
    } else {
      throw new Error(`Failed to extract layer: ${error.message}`);
    }
  }
}

// Alternative method using gdal-async library (if you prefer programmatic approach)
async function extractLayerFeaturesWithGDAL(inputPath, layerName, transformCoords = true) {
  const gdal = require('gdal-async');
  
  try {
    console.log(`Opening dataset: ${inputPath}`);
    const dataset = await gdal.openAsync(inputPath);
    
    console.log(`Getting layer: ${layerName}`);
    const layer = dataset.layers.get(layerName);
    
    if (!layer) {
      const availableLayers = dataset.layers.map(l => l.name).join(', ');
      throw new Error(`Layer "${layerName}" not found. Available layers: ${availableLayers}`);
    }
    
    // Set up coordinate transformation if needed
    let coordTrans = null;
    if (transformCoords && layer.srs) {
      const targetSRS = gdal.SpatialReference.fromEPSG(4326);
      coordTrans = new gdal.CoordinateTransformation(layer.srs, targetSRS);
    }
    
    const features = [];
    let featureCount = 0;
    
    // Use GDAL's iterator instead of manual coordinate parsing
    await layer.features.forEachAsync(async (feature) => {
      try {
        let geometry = feature.getGeometry();
        
        // Transform geometry using GDAL's built-in method
        if (coordTrans && geometry) {
          geometry = geometry.clone();
          geometry.transform(coordTrans);
        }
        
        // Convert to GeoJSON using GDAL's method (eliminates manual parsing)
        const geoJSONGeometry = geometry ? geometry.toObject() : null;
        
        features.push({
          fid: feature.fid,
          geometry_type: geoJSONGeometry?.type || 'Unknown',
          coordinates: geoJSONGeometry?.coordinates,
          properties: feature.fields.toObject(),
          geometry: geoJSONGeometry
        });
        
        featureCount++;
      } catch (featureError) {
        console.warn(`Skipping feature ${feature.fid}: ${featureError.message}`);
      }
    });
    
    console.log(`Successfully processed ${featureCount} features`);
    
    return {
      name: layerName,
      geometry_type: features.length > 0 ? features[0].geometry_type : 'Unknown',
      feature_count: featureCount,
      coordinate_transformation_applied: transformCoords,
      features: features,
      spatial_reference: transformCoords ? {
        transformed: 'EPSG:4326 (WGS84)',
        original: layer.srs ? layer.srs.toWKT() : 'Unknown'
      } : null,
      extraction_summary: {
        total_extracted: featureCount,
        truncated: false,
        max_feature_limit: null
      }
    };
    
  } catch (error) {
    console.error('GDAL extraction error:', error);
    throw error;
  }
}

module.exports = { extractLayerFeatures, extractLayerFeaturesWithGDAL };