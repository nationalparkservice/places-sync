var reproject = require('reproject');
var epsg = require('proj4js-defs');

module.exports = function (geojson) {
  var geoJsonObj;
  var crss = getEpsgDefs();

  // Parse or clone the GeoJSON file
  geoJsonObj = JSON.parse(typeof geojson === 'string' ? geojson : JSON.stringify(geojson));

  // Reproject the GeoJSON File
  geoJsonObj = reproject.reproject(geoJsonObj, null, 'EPSG:4326', crss);

  // convert the geojson to rows for the table
  return geojsonToRows(geoJsonObj);
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
      var type = Object.prototype.toString.call(feature.properties[property]).slice(8, -1);
      var transforms = {
        'Array': function (value) {
          return JSON.stringify(value);
        },
        'Object': function (value) {
          return JSON.stringify(value);
        },
        'String': function (value) {
          return value;
        },
        'Date': function (value) {
          return value.toUTCString();
        },
        'Error': function (value) {
          return value.toString();
        },
        'RegExp': function (value) {
          return value.toString();
        },
        'Function': function (value) {
          return value.toString();
        },
        'Boolean': function (value) {
          return value ? 1 : 0;
        },
        'Number': function (value) {
          return value;
        },
        'Null': function (value) {
          return null;
        },
        'Undefined': function (value) {
          return null;
        }
      };
      properties[property] = transforms[type] ? transforms[type](feature.properties[property]) : feature.properties[property].toString();
    }
    properties['geometry'] = JSON.stringify(feature.geometry);
    return properties;
  });
  return rows;
};
