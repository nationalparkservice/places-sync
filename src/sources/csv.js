/* CSV File:
 *   The CSV file MUST have headers
 *   *filePath: (path the the csv file)
 *   encoding: (defaults to 'UTF8')
 */

var Promise = require('bluebird');
var csv = require('csv');
var Immutable = require('immutable');
var tools = require('../tools');
var castToSqliteType = require('./helpers/castToSqliteType');
var fs = Promise.promisifyAll(require('fs'));

var readCsv = function (data) {
  var jsonData;
  var columns;

  return new Promise(function (fulfill, reject) {
    csv.parse(data, function (e, r) {
      if (e) {
        reject(e);
      } else {
        // // Remove the first row, which should be headers
        jsonData = castToSqliteType(r.slice(1) || []);

        columns = r[0].map(function (name, index) {
          return {
            'name': name,
            'nativeType': 'text'
          };
        });

        fulfill({
          'data': jsonData.map(function (row) {
            return tools.nameArrayValues(row, r[0]);
          }),
          'columns': columns
        });
      }
    });
  });
};

module.exports = function (sourceConfig) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    if (typeof connectionConfig.get('filePath') !== 'string') {
      throw new Error('filePath must be defined for a CSV file');
    }
    connectionConfig = connectionConfig.set('encoding', connectionConfig.get('encoding') || 'UTF8');

    // Define the taskList
    var tasks = [{
      'name': 'openFile',
      'description': 'Does a few checks on opens the file if it can',
      'task': fs.readFileAsync,
      'params': [connectionConfig.get('filePath'), connectionConfig.get('encoding')]
    }, {
      'name': 'convertFromCsv',
      'description': 'Takes the source data and converts it to Json',
      'task': readCsv,
      'params': ['{{openFile}}']
    }];
    tools.iterateTasks(tasks, 'csv').then(function (r) {
      var columns = r[1].columns.map(function (column) {
        column.primaryKey = tools.arrayify(sourceConfig.primaryKey).indexOf(column.name) !== -1;
        column.lastUpdated = tools.arrayify(sourceConfig.lastUpdated).indexOf(column.name) !== -1;
        return column;
      });
      fulfill({
        'data': r[1].data,
        'columns': columns
      });
    }).catch(reject);
  });
};
