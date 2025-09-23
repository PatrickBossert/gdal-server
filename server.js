// CORRECT APPROACH - Use ogr2ogr for automatic geometry handling
// Replace the extractLayerFeatures function in your server.js

const { exec } = require('child_process');
const { promisify } = require('util');
const fs = require('fs').promises;
const path = require('path');

const execAsync = promisify(exec);

async function extractLayerFeatures(inputPath, layerName, transformCoords = true) {
  try {
    console.log(`Extracting layer: ${layerName} from ${inputPath}`);
    console.log(`Transform coordinates: ${transformCoords}`);
    
    // Create temporary output file
    const outputPath = `/tmp/extracted_${Date.now()}_${Math.random().toString(36).substr(2, 9)}.geojson`;
    
    // Build ogr2ogr command - this automatically fixes geometry issues
    const cmd = [
      'ogr2ogr',
      '-f', 'GeoJSON',
      `"${outputPath}"`,
      `"${inputPath}"`,
      `"${layerName}"`
    ];
    
    // Add coordinate transformation if requested
    if (transformCoords) {
      cmd.push('-t_srs', 'EPSG:4326');
    }
    
    // Add options for robustness
    cmd.push('-skipfailures');  // Skip invalid geometries
    cmd.push('-lco', 'RFC7946=YES');  // Standard GeoJSON format
    
    const fullCommand = cmd.join(' ');
    console.log('Executing command:', fullCommand);
    
    // Execute ogr2ogr command
    const { stdout, stderr } = await execAsync(fullCommand, {
      maxBuffer: 50 * 1024 * 1024  // 50MB buffer
    });
    
    if (stderr && !stderr.includes('Warning')) {
      console.warn('ogr2ogr stderr:', stderr);
    }
    
    // Read the generated GeoJSON file
    const geojsonContent = await fs.readFile(outputPath, 'utf8');
    const geojsonData = JSON.parse(geojsonContent);
    
    // Clean up temporary file
    await fs.unlink(outputPath);
    
    // Process the results
    const features = geojsonData.features || [];
    
    const result = {
      name: layerName,
      geometry_type: features.length > 0 ? features[0]?.geometry?.type : 'Unknown',
      feature_count: features.length,
      coordinate_transformation_applied: transformCoords,
      features: features.map((feature, index) => ({
        fid: feature.id || index + 1,
        geometry_type: feature.geometry?.type,
        coordinates: feature.geometry?.coordinates,
        properties: feature.properties || {},
        geometry: feature.geometry
      })),
      spatial_reference: transformCoords ? {
        transformed: 'EPSG:4326 (WGS84)',
        original: 'Auto-detected from source'
      } : null,
      extraction_summary: {
        total_extracted: features.length,
        truncated: false,
        max_feature_limit: null
      }
    };
    
    console.log(`Successfully extracted ${features.length} features from layer: ${layerName}`);
    return result;
    
  } catch (error) {
    console.error('Layer extraction error:', error);
    
    // Handle specific ogr2ogr errors
    if (error.message.includes('Layer') && error.message.includes('does not exist')) {
      throw new Error(`Layer "${layerName}" not found in the dataset.`);
    } else if (error.message.includes('Unable to open')) {
      throw new Error(`Cannot open input file: ${inputPath}`);
    } else {
      throw new Error(`GDAL extraction failed: ${error.message}`);
    }
  }
}