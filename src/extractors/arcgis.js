var terraformer = require('terraformer-arcgis-parser');
var datawrap = require('datawrap');
var request = datawrap.Bluebird.promisify(require('request'));
var tools = require('../tools');
datawrap.Bluebird.promisifyAll(request);

var arcgis = module.exports = function (source) {
  var sourceUrl = source.data;
  var predefinedColumns = tools.desimplifyArray(source.columns);
  var lastEditDate = source.lastEditDate;

  return new datawrap.Bluebird(function (fulfill, reject) {
    var taskList = [{
      'name': 'Read source',
      'task': request,
      'params': [sourceUrl + '?f=pjson']
    }, {
      // Query the service
      'name': 'Query service',
      'task': queryService,
      'params': ['{{Read source}}', predefinedColumns, lastEditDate, sourceUrl]
    }];

    datawrap.runList(taskList).then(function (result) {
      fulfill(result[result.length - 1]);
    }).catch(function (error) {
      reject(error[error.length - 1]);
    });
  });
};

var queryService = function (sourceData, predefinedColumns, lastEditDate, sourceUrl) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var sourceInfo = getSourceInfo(sourceData, predefinedColumns, sourceUrl);
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

    request(tools.buildUrlQuery(sourceUrl + 'query?', queries.getCount)).then(function (countResult) {
      var requests = [];
      var count = JSON.parse(countResult.body).count;
      for (var i = 0; i < count; i += queries.baseQuery.resultRecordCount) {
        queries.baseQuery.resultOffset = i;
        requests.push(tools.buildUrlQuery(sourceUrl + 'query?', queries.baseQuery));
      }
      datawrap.runList(requests.map(function (req) {
        return {
          'name': 'Query ' + req,
          'task': request,
          'params': [req]
        };
      })).then(function (baseResults) {
        var esriJson = JSON.parse(baseResults[0].body);

        esriJson.features = [];
        baseResults.forEach(function (baseResult) {
          JSON.parse(baseResult.body).features.forEach(function (row) {
            esriJson.features.push(row);
          });
        });

        var geoJson = esriToGeoJson(esriJson, {
          'sr': (esriJson.spatialReference && (esriJson.spatialReference.latestWkid || esriJson.spatialReference.wkid)) || queries.baseResult.outSR,
          'idAttribute': sourceInfo.primaryKey
        });

        sourceInfo.data = JSON.stringify(geoJson);
        fulfill(sourceInfo);
      }).catch(function (e) {
        reject(tools.arrayify(e)[tools.arrayify(e).length - 1]);
      });
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

var getSourceInfo = function (sourceData, predefinedColumns, sourceUrl) {
  sourceData = JSON.parse(sourceData.body);
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
    sourceInfo.editFields = {
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
  return sourceInfo;
};

var parseEsriType = function (esriType) {
  var returnValue;
  var types = {
    'integer': ['esriFieldTypeOID', 'esriFieldTypeSmallInteger', 'esriFieldTypeDate', 'esriFieldTypeInteger'],
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
