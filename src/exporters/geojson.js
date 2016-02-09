var datawrap = require('datawrap');
var Mockingbird = datawrap.mockingbird;
var fs = require('fs');
var mainDefaults = require('../../defaults');
var tools = require('../tools');

module.exports = function (destination, defaults) {
  defaults = defaults || mainDefaults;
  var initialized = false;
  var filePath;

  var initialize = function (destination, callback) {
    // Creates a blank file, and makes sure the directory exists and stuff
    return new (Mockingbird(callback))(function (fulfill, reject) {
      if (initialized) {
        fulfill(filePath);
      } else {
        var regexps = defaults.regexps;
        var dataDirectory = defaults.dataDirectory;
        // if it starts with file:///, replace the path`
        var tmpFilePath = destination.data.replace(new RegExp(regexps['file']), dataDirectory);
        // Try to create the file
        fs.appendFile(tmpFilePath, '', function (appendE, appendR) {
          if (appendE) {
            appendE.message = 'Cannot open file: ' + tmpFilePath + ' -- ' + appendE.message;
            reject(appendE);
          } else {
            filePath = tmpFilePath;
            fulfill(filePath);
          }
        });
      }
    });
  };

  var getData = function (data) {
    var origin = {
      'source': data.source || data._source || {},
      'where': data.where
    };
    if (typeof origin.source._source === 'function') {
      return origin.source.getDataWhere(origin.where);
    } else {
      // Hopefully the data was passed in in the right format!
      return tools.syncPromise(function () {
        return data;
      });
    }
  };

  var convertGeometries = function (sourceData, results) {
    var source = sourceData.source || sourceData._source || {};
    var geometryColumns = typeof source._source === 'function' && source._source().geometryColumns;
    if (!geometryColumns) {
      // Gues the geometry columns from the results
      // TODO: this will probably require magic
    }
    var newData = results.map(function (row) {
      var geometry = {};
      var newRow = {
        'type': 'Feature',
        'geometry': {},
        'properties': JSON.parse(JSON.stringify(row))
      };
      geometryColumns.forEach(function (geoColumn) {
        if (newRow.properties[geoColumn.name] !== undefined) {
          geometry[geoColumn.type] = newRow.properties[geoColumn.name];
          delete newRow.properties[geoColumn.name];
        }
      });
      newRow.geometry = toGeoJson(geometry);
      return newRow;
    });
    // TODO: projections and stuff
    return {
      'type': 'FeatureCollection',
      'features': newData
    };
  };

  var toGeoJson = function (geom) {
    // TODO, this only does points and doesn't really do any checking for anything
    return {
      'type': 'Point',
      'coordinates': [parseFloat(geom.longitude || '0', 10), parseFloat(geom.latitude || '0', 10)]
    };
  };

  var writeFile = function (outFile, dataToWrite) {
    dataToWrite = typeof dataToWrite === 'string' ? dataToWrite : JSON.stringify(dataToWrite);
    return new datawrap.Bluebird(function (fulfill, reject) {
      fs.writeFile(outFile, dataToWrite, function (writeE, writeR) {
        if (writeE) {
          reject(writeE);
        } else {
          fulfill(writeR);
        }
      });
    });
  };

  var addData = function (data, destination, callback) {
    return new (Mockingbird(callback))(function (fulfill, reject) {
      var taskList = [{
        'name': 'initialize file',
        'description': 'Makes sure that the files exists or that we can write to it',
        'task': initialize,
        'params': [destination]
      }, {
        'name': 'get source data',
        'description': 'Gets the data from the source for us',
        'task': getData,
        'params': [data]
      }, {
        'name': 'convert geometries',
        'description': 'Creates GeoJSON from the geometry columns',
        'task': tools.syncPromise(convertGeometries),
        'params': [data, '{{get source data}}']
      }, {
        'name': 'write to file',
        'description': 'Creates GeoJSON from the geometry columns',
        'task': writeFile,
        'params': ['{{initialize file}}', '{{convert geometries}}']
      }];
      datawrap.runList(taskList).then(function (r) {
        fulfill(Array.isArray(r) ? r[r.length - 1] : r);
      }).catch(function (e) {
        reject(Array.isArray(e) ? e[e.length - 1] : e);
      });
    });
  };

  return {
    'initialize': function (callback) {
      return initialize(destination, callback);
    }, // Runs a function to get info from geojson on this table
    'addData': function (data, callback) {
      return addData(data, destination, callback);
    }, // Runs a function to INSERT new data
    'removeData': {} // Accepts an array of primary keys to remove
  };
};
