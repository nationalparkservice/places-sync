var terraformer = require('terraformer-arcgis-parser');
var datawrap = require('datawrap');
var runList = datawrap.runList;
var Bluebird = datawrap.Bluebird;
var superagent = require('superagent');
var tools = require('../tools');

module.exports = function (source, regexps) {
  var sourceUrl = source.data.replace(new RegExp(regexps[source.extractionType]), '');
  sourceUrl += sourceUrl.substr(-1) === '/' ? '' : '/';
  var lastEditDate = source.lastEditDate;

  return new Bluebird(function (fulfill, reject) {
    var taskList = [{
      'name': 'Read source',
      'task': getAsync,
      'params': [sourceUrl, {
        'f': 'json'
      }]
    }, {
      // Query the service
      'name': 'Query service',
      'task': queryService,
      'params': [source, '{{Read source}}', lastEditDate, sourceUrl]
    }];

    runList(taskList).then(function (result) {
      fulfill(result[result.length - 1]);
    }).catch(function (error) {
      reject(error[error.length - 1]);
    });
  });
};

var queryService = function (originalSource, sourceData, lastEditDate, sourceUrl) {
  return new Bluebird(function (fulfill, reject) {
    var sourceInfo = getSourceInfo(originalSource, sourceData, sourceUrl);
    var queries = {
      'getCount': {
        'where': '1=1',
        'returnCountOnly': 'true',
        'f': 'json'
      },
      'baseQuery': {
        'where': '1=1',
        'outFields': tools.simplifyArray(sourceInfo.columns).join(','),
        'returnGeometry': 'true',
        'outSR': '4326',
        'resultOffset': '0',
        'resultRecordCount': sourceInfo.maxRecordCount,
        'f': 'json'
      }
    };

    getAsync(sourceUrl + 'query', queries.getCount).then(function (countResultRaw) {
      var countResult = JSON.parse(countResultRaw.text);
      if (!countResult.error) {
        var requests = [];
        var count = countResult.count;
        for (var i = 0; i < count; i += queries.baseQuery.resultRecordCount) {
          queries.baseQuery.resultOffset = i;
          requests.push([sourceUrl + 'query', queries.baseQuery]);
        }
        runList(requests.map(function (req) {
          return {
            'name': 'Query ' + req,
            'task': getAsync,
            'params': req
          };
        })).then(function (baseResults) {
          var esriJson = JSON.parse(baseResults[0].text);
          var sr = (esriJson.spatialReference && (esriJson.spatialReference.latestWkid || esriJson.spatialReference.wkid)) || queries.baseResult.outSR;

          esriJson.features = [];
          baseResults.forEach(function (baseResult) {
            JSON.parse(baseResult.text).features.forEach(function (row) {
              esriJson.features.push(row);
            });
          });

          var geoJson = esriToGeoJson(esriJson, {
            'sr': sr,
            'idAttribute': sourceInfo.primaryKey
          });

          // Really make sure we put the right spatial reference here
          geoJson.crs = geoJson.crs || {
            'type': 'name',
            'properties': {
              'name': 'EPSG:' + sr
            }
          };
          geoJson.crs.properties = geoJson.crs.properties || {
            'name': 'EPSG:' + sr
          };

          sourceInfo.data = JSON.stringify(geoJson);
          fulfill(sourceInfo);
        }).catch(function (e) {
          reject(tools.arrayify(e)[tools.arrayify(e).length - 1]);
        });
      } else {
        reject(new Error(countResult.error.code + ' ' + countResult.error.message + ' ' + countResult.error.details));
      }
    });
  });
};

var esriToGeoJson = function (esriJson, options) {
  // This accepts a full esriJson object from an esri endpoint
  // the options are defined here: http://terraformer.io/arcgis-parser/#arcgisparse

  // Create a skeleton geojson obj
  var geojson = {
    'type': 'FeatureCollection'
  };

  // If the sr is specified, add it in
  if (options.sr) {
    geojson.crs = {
      'type': 'name',
      'properties': {
        'name': 'EPSG:' + options.sr
      }
    };
  }

  var createFeature = function (esriFeature, esriOptions) {
    return {
      'type': 'Feature',
      'id': esriFeature[esriOptions.idAttribute || 'OBJECTID'],
      'geometry': terraformer.parse(esriFeature.geometry, esriOptions),
      'properties': esriFeature.attributes
    };
  };

  geojson.features = esriJson.features.map(function (feature) {
    return createFeature(feature, options);
  });

  return geojson;
};

var getSourceInfo = function (originalSource, sourceDataRaw, sourceUrl) {
  var sourceData = JSON.parse(sourceDataRaw.text);
  var predefinedColumns = tools.desimplifyArray(originalSource.columns);
  var sourceInfo = {
    'columns': [],
    'description': sourceData.description,
    'editingInfo': {},
    'format': 'geojson',
    'lastEditDate': sourceData.editingInfo.lastEditDate,
    'maxRecordCount': sourceData.maxRecordCount,
    'name': sourceData.name,
    'primaryKey': sourceData.globalIdField,
    'sourceUrl': sourceUrl,
    'supportsPagination': sourceData.advancedQueryCapabilities.supportsPagination
  };
  if (sourceData.editingInfo) {
    sourceInfo.editingInfo = {
      'dateCreated': sourceData.editFieldsInfo.creationDateField,
      'userCreated': sourceData.editFieldsInfo.creatorField,
      'dateEdited': sourceData.editFieldsInfo.editDateField,
      'userEdited': sourceData.editFieldsInfo.editorField
    };
  }

  sourceData.fields.forEach(function (field) {
    var column = {
      'name': field.name
    };
    if (predefinedColumns) {
      predefinedColumns.forEach(function (c) {
        if (c.name === field) {
          column.type = c.type;
        }
      });
    }
    column.esriType = field.type;
    column.type = column.type || parseEsriType(field.type);
    sourceInfo.columns.push(column);
  });

  // We are also adding a geometry column to the table now
  sourceInfo.columns.push({
    'name': 'geometry',
    'type': 'text',
    'esriType': 'json'
  });
  for (var field in sourceInfo) {
    originalSource[field] = originalSource[field] || sourceInfo[field];
  }
  return originalSource;
};

var getAsync = function (url, query) {
  return new Bluebird(function (fulfill, reject) {
    superagent.get(url).query(query).end(function (err, res) {
      if (err) {
        reject(err);
      } else {
        fulfill(res);
      }
    });
  });
};

var parseEsriType = function (esriType) {
  var returnValue;
  var types = {
    'integers': ['esriFieldTypeOID', 'esriFieldTypeSmallInteger', 'esriFieldTypeDate', 'esriFieldTypeInteger'],
    'floats': ['esriFieldTypeDouble', 'esriFieldTypeSingle'],
    'texts': ['esriFieldTypeString', 'esriFieldTypeGlobalID', 'esriFieldTypeBlob', 'esriFieldTypeRaster', 'esriFieldTypeGUID', 'esriFieldTypeXML']
  };
  for (var type in types) {
    if (types[type].indexOf(esriType) > -1) {
      returnValue = type.slice(0, -1);
      break;
    }
  }
  return returnValue;
};
