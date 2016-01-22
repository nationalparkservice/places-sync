var datawrap = require('datawrap');
var request = datawrap.Bluebird.promisify(require('request'));
var fileDb = require('../fileDb');
var sourceTemplate = require('./sqliteTemplate');

datawrap.Bluebird.promisifyAll(request);

var arcgis = module.exports = function (layerUrl, columns, customFunctions) {
  // Columns needs to include a primary key, all other columns will be pulled from the layerURL result
  var regexp = new RegExp('\\?.+$');
  layerUrl = layerUrl.replace(regexp, '');

  return new datawrap.Bluebird(function (fulfill, reject) {
    var taskList = [{
      // Read the layerURL with ?f=pjson at the end if it's not already there
      'name': 'Read layer',
      'task': request,
      'params': [layerUrl + '?f=pjson']
    }, {
      // Query the service
      'name': 'Query service',
      'task': queryToDb,
      'params': ['{{Read layer}}', layerUrl]
    }, {
      // Create a table
      'name': 'Create database',
      'task': fileDb,
      'params': ['{{Query service}}']
    }];

    datawrap.runList(taskList)
      .then(function (d) {
        var layerInfo = getLayerInfo(d[0], layerUrl);
        fulfill(sourceTemplate(layerInfo.name, {
          'primaryKey': layerInfo.primaryKey,
          'columns': layerInfo.columns.map(function (d) {
            return d.name;
          }),
          'lastUpdated': layerInfo.lastEditDate
        }, d[2].database));
      })
      .catch(function(e){
        reject (e[e.length-1]);
        throw(e[e.length-1]);
      });
  });
};

var queryToDb = function (layerData, layerUrl) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var layerInfo = getLayerInfo(layerData, layerUrl);
    var getCount = 'where=1=1&returnCountOnly=true&f=json';
    var baseQuery = 'where=1=1&outFields={{columns}}&returnGeometry=true&outSR=4326&resultOffset={{offset}}&resultRecordCount={{recordCount}}&f=geojson';
    var columns = layerInfo.columns.map(function (column) {
      return column.name;
    }).join(',');
    var dbInfo = {
      'name': layerInfo.name,
      'data': {
        'type': 'geojson',
        'columns': layerInfo.columns,
        'data': {}
      }
    };
    request(layerInfo.layerUrl + 'query?' + getCount)
      .then(function (response) {
        var queries = [];
        var i = 0;
        var count = JSON.parse(response.body).count;

        for (i = 0; i < count; i += layerInfo.maxRecordCount) {
          queries.push(datawrap.fandlebars(baseQuery, {
            'columns': columns,
            'offset': i,
            'recordCount': layerInfo.maxRecordCount
          }));
        }
        datawrap.runList(queries.map(function (query, index) {
          return {
            'name': 'Query ' + index,
            'task': request,
            'params': [layerInfo.layerUrl + 'query?' + query]
          };
        })).then(function (results) {
          var geojson = JSON.parse(results[0].body);
          if (geojson.properties) {
            geojson.properties.exceededTransferLimit = false;
          }
          geojson.features = [];
          results.forEach(function (data) {
            JSON.parse(data.body).features.forEach(function (row) {
              geojson.features.push(row);
            });
          });
          dbInfo.data.data = geojson;
          dbInfo.data = [JSON.stringify(dbInfo.data.data)];
          fulfill(dbInfo);
        })
          .catch(reject);
      })
      .catch(reject);
  });
};

var getLayerInfo = function (layerData, layerUrl) {
  layerData = JSON.parse(layerData.body);
  return {
    columns: layerData.fields.map(function (field) {
      return {
        'name': field.name,
        'esriType': field.type,
        'type': parseEsriType(field.type)
      };
    }),
    description: layerData.description,
    lastEditDate: layerData.editingInfo.lastEditDate,
    layerUrl: layerUrl,
    maxRecordCount: layerData.maxRecordCount,
    name: layerData.name,
    primaryKey: layerData.globalIdField,
    supportsPagination: layerData.advancedQueryCapabilities.supportsPagination,
    tableName: layerData.name + '_' + layerData.serviceItemId
  };
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

 arcgis('http://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/ZION/FeatureServer/19/');
