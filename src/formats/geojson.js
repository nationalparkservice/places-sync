var reproject = require('reproject');
var epsg = require('proj4js-defs');
var toJsonTable = require('./json');

module.exports = function (geojson) {
  var geoJsonObj;
  var crss = getEpsgDefs();

  // Parse or clone the GeoJSON file
  geoJsonObj = JSON.parse(typeof geojson === 'string' ? geojson : JSON.stringify(geojson));

  // Reproject the GeoJSON File
  geoJsonObj = reproject.reproject(geoJsonObj, null, 'EPSG:4326', crss);

  // convert the geojson to rows for the table
  return toJsonTable(geojsonToRows(geoJsonObj));
};

var getEpsgDefs = function () {
  var temp = {
    defs: {}
  };
  epsg(temp);
  return temp.defs;
};

var geojsonToRows = function (geojson) {
  var features = geojson.features;
  var property;
  var rows = features.map(function (feature) {
    var properties = {};
    for (property in feature.properties) {
      properties[property] = feature.properties[property];
    }
    properties['geometry'] = JSON.stringify(feature.geometry);
    return properties;
  });
  return rows;
};
