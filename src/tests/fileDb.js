var defaults = {
  'fileDesignator': 'file:///',
  'fileOptions': {
    'encoding': 'utf8'
  },
  'dataDirectory': __dirname + '/data',
  'rootDirectory': __dirname
};
var fileDb = require('../fileDb');
var tools = require('../tools');
var datawrap = require('datawrap');
var connection = {
  'type': 'sqlite',
  'data': {
    'test': {
      'data': 'file:///csvDbtest.csv',
      'format': 'csv'
    },
    'test2': 'file:///csvDbtest2.csv',
    'test3': 'file:///csvDbtest3.csv',
    'jsonTest': 'file:///jsonDbtest.json',
    'jsonTestGeo': 'file:///geojsonTest.json'
  },
  'format': 'csv',
  'name': 'csvTest'
};
var db = datawrap(connection);
var tableNames = [];
var fs = datawrap.Bluebird.promisifyAll(require('fs'));
var geojsonToJsonTable = require('../geojsonToJsonTable');
var csv = require('csv');
var tape = require('tape');

// Pull the tableName from the data field
for (var value in connection.data) {
  tableNames.push(value);
}

// Add the info to the database
tape('Check Tables', function (t) {
  var reportError = function (e) {
    e = tools.arrayify(e);
    console.error(tools.readOutput(e));
    console.error(e[e.length - 1]);
    console.log('failure');
    t.end();
    throw e[e.length - 1];
  };
  fileDb(connection, defaults).then(function (a) {
    // If it goes in ok, make sure it's correct
    var taskList = tableNames.map(function (tableName) {
      return {
        'name': 'Read ' + tableName,
        'task': db.runQuery,
        'params': ['SELECT * FROM "' + tableName + '";']
      };
    });
    tableNames.forEach(function (tableName) {
      taskList.push({
        'name': 'Check ' + tableName,
        'task': tableName.substr(0, 4) === 'json' ? compareJson : compareCsv,
        'params': [tableName, '{{Read ' + tableName + '}}', t]
      });
    });
    taskList.push({
      'name': 'Close database',
      'task': db.runQuery,
      'params': [null, null, {
        'close': true
      }]
    });
    datawrap.runList(taskList)
      .then(function (r) {
        t.end();
        console.log('Success!');
      })
      .catch(reportError);
  }).catch(reportError);
});
var compareJson = function (tableName, data, t) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var jsonFilePath = (typeof connection.data[tableName] === 'string' ? connection.data[tableName] : connection.data[tableName].path || connection.data[tableName].data).replace(defaults.fileDesignator, defaults.dataDirectory + '/');
    fs.readFileAsync(jsonFilePath).then(function (fileData) {
      fileData = JSON.parse(fileData);
      if (fileData.type && fileData.type === 'FeatureCollection') {
        fileData = geojsonToJsonTable(fileData);
      }
      fileData.forEach(function (row, rowIndex) {
        t.deepEqual(data[0][rowIndex], row);
      });
      fulfill([]);
    }).catch(reject);
  });
};
var compareCsv = function (tableName, data, t) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var csvFilePath = (typeof connection.data[tableName] === 'string' ? connection.data[tableName] : connection.data[tableName].path || connection.data[tableName].data).replace(defaults.fileDesignator, defaults.dataDirectory + '/');
    fs.readFileAsync(csvFilePath).then(function (fileData) {
      csv.parse(fileData, function (e, r) {
        var columns = [];
        var csvData;
        var errors = [];
        if (!e) {
          columns = r[0];
          csvData = r.slice(1);
          data[0].forEach(function (row, rowI) {
            columns.forEach(function (column, columnI) {
              var testValue = csvData[rowI][columnI];
              if (typeof row[column] !== 'string') {
                testValue = parseFloat(testValue, 10);
              }
              t.equal(testValue, row[column]);
              if (testValue !== row[column]) {
                errors.push({
                  'row': rowI,
                  'columnName': column,
                  'column': columnI,
                  'testValue': testValue,
                  'csvValue': csvData[rowI][columnI],
                  'dbValue': row[column]
                });
              }
            });
          });
          fulfill(errors);
        } else {
          reject(e);
        }
      });
    }).catch(reject);
  });
};
