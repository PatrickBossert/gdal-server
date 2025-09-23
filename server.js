// Replace ONLY the extractLayerFeatures function in your server.js with this version
// This uses proper GDAL methods to avoid coordinate parsing errors

async function extractLayerFeatures(inputPath, layerName, transformCoords = true) {
  const gdal = require('gdal-async');
  
  try {
    console.log(`Extracting layer: ${layerName}`);
    
    const dataset = await gdal.openAsync(inputPath);
    const layer = dataset.layers.get(layerName);
    
    if (!layer) {
      throw new Error(`Layer "${layerName}" not found`);
    }
    
    const features = [];
    
    // Use GDAL's safe iteration method
    layer.features.forEach((feature) => {
      try {
        // Get geometry safely using GDAL methods
        const geometry = feature.getGeometry();
        let geoJSONGeometry = null;
        
        if (geometry) {
          // Use GDAL's toObject() method - this avoids manual coordinate parsing
          geoJSONGeometry = geometry.toObject();
          
          // Apply coordinate transformation if requested
          if (transformCoords && layer.srs) {
            try {
              const targetSRS = gdal.SpatialReference.fromEPSG(4326);
              const coordTrans = new gdal.CoordinateTransformation(layer.srs, targetSRS);
              const clonedGeom = geometry.clone();
              clonedGeom.transform(coordTrans);
              geoJSONGeometry = clonedGeom.toObject();
            } catch (transformErr) {
              console.warn('Transform failed, using original coordinates');
            }
          }
        }
        
        features.push({
          fid: feature.fid || features.length + 1,
          geometry_type: geoJSONGeometry?.type || null,
          coordinates: geoJSONGeometry?.coordinates || null,
          properties: feature.fields.toObject(),
          geometry: geoJSONGeometry
        });
        
      } catch (featureError) {
        console.warn(`Skipping feature: ${featureError.message}`);
      }
    });
    
    return {
      name: layerName,
      geometry_type: features.length > 0 ? features[0].geometry_type : 'Unknown',
      feature_count: features.length,
      coordinate_transformation_applied: transformCoords,
      features: features,
      spatial_reference: transformCoords ? {
        transformed: 'EPSG:4326 (WGS84)',
        original: 'Auto-detected'
      } : null,
      extraction_summary: {
        total_extracted: features.length,
        truncated: false,
        max_feature_limit: null
      }
    };
    
  } catch (error) {
    console.error('Layer extraction error:', error);
    throw new Error(`Failed to extract layer "${layerName}": ${error.message}`);
  }
}