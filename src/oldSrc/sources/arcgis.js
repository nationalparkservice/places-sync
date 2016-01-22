var arcgis = function(layerUrl, columns, customFunctions) {
  // Columns needs to include a primary key, all other columns will be pulled from the layerURL result
  var taskList = [];

  // Read the layerURL with ?f=pjson at the end if it's not already there
  // Extract information from this document
    // Column Names
  // Query the service
  // TODO: customFunctions can define to have a different where clause
  // Go through the JSON and clean it up (may require reprojecting to WGS84)
  // Create a table
  // Return the source object
};
