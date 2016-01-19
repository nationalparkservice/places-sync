var defaults = {
  'fileDesignator': 'file:///',
  'fileOptions': {
    'encoding': 'utf8'
  },
  'csvDirectory': __dirname + '/csv',
  'rootDirectory': __dirname
};
var csvDb = require('../csvDb');
var tools = require('../tools');
var datawrap = require('datawrap');
var connection = {
  'type': 'sqlite',
  'data': {
    'test': 'file:///test.csv',
    'test2': 'file:///test2.csv',
    'test3': 'file:///test3.csv'
  },
  'name': 'csvTest'
};
var db = datawrap(connection);
var tableNames = [];
var fs = datawrap.Bluebird.promisifyAll(require('fs'));
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
  csvDb(connection, defaults).then(function (a) {
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
        'task': compareCsv,
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

var compareCsv = function (tableName, data, t) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var csvFilePath = connection.data[tableName].replace(defaults.fileDesignator, defaults.csvDirectory + '/');
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
