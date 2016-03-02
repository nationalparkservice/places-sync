/* JSON File:
 *   *filePath: (path the the json file)
 *   encoding: (defaults to 'UTF8')
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = require('../tools');
var updateJsonSource = require('./helpers/updateJsonSource');
var fs = Promise.promisifyAll(require('fs'));

var WriteFn = function (data, columns, filePath, fileEncoding) {
  return function (updated, removed) {
    return new Promise(function (fulfill, reject) {
      updateJsonSource(data, updated, removed)
        .then(function (newData) {
          return writeJson(newData, columns, filePath, fileEncoding).then(fulfill).catch(reject);
        })
        .catch(reject);
    });
  };
};

var writeJson = function (data, columns, filePath, fileEncoding) {
  data = data.map(function (row) {
    var newRow = {};
    columns.forEach(function (column) {
      newRow[column.name] = tools.isUndefined(data[column.name], tools.isUndefined(column.defaultValue, ''));
    });
  });
  return fs.writeFileAsync(filePath, data, fileEncoding);
};

var readJson = function (data, predefinedColumns) {
  data = typeof data === 'string' ? JSON.parse(data) : data;
  var columns = predefinedColumns || [];
  if (!predefinedColumns) {
    data.forEach(function (row) {
      var thisColumn;
      for (var column in row) {
        thisColumn = columns.filter(function (value) {
          return value.name === column;
        })[0];
        if (!thisColumn) {
          var newColumnIdx = columns.push({
            'name': column
          }) - 1;
          thisColumn = columns[newColumnIdx];
        }
        thisColumn.nativeType = tools.getDataType(row[column], thisColumn.type || 'interger', typeof row[column]);
      }
    });
  }
  return {
    'data': data,
    'columns': columns
  };
};

module.exports = function (sourceConfig) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    if (typeof connectionConfig.get('filePath') !== 'string' && connectionConfig.get('data') === undefined) {
      throw new Error('data or filePath must be defined for a JSON file');
    }
    connectionConfig = connectionConfig.set('encoding', connectionConfig.get('encoding') || 'UTF8');

    var returnData = function () {
      return connectionConfig.get('data');
    };

    // Define the taskList
    var tasks = [{
      'name': 'openFile',
      'description': 'Does a few checks on opens the file if it can',
      'task': connectionConfig.get('data') === undefined ? fs.readFileAsync : returnData,
      'params': [connectionConfig.get('filePath'), connectionConfig.get('encoding')]
    }, {
      'name': 'createDatabaseFromJson',
      'description': 'Creates a sqlite database from Json data',
      'task': readJson,
      'params': ['{{openFile}}', sourceConfig.columns]
    }];
    tools.iterateTasks(tasks).then(function (r) {
      var columns = r[1].columns.map(function (column) {
        column.primaryKey = tools.arrayify(sourceConfig.primaryKey).indexOf(column.name) !== -1;
        column.lastUpdateField = tools.arrayify(sourceConfig.lastUpdateField).indexOf(column.name) !== -1;
        column.removedField = tools.arrayify(sourceConfig.removedField).indexOf(column.name) !== -1;
        return column;
      });
      fulfill({
        'data': r[1].data,
        'columns': columns,
        'writeFn': new WriteFn(r[1].data, columns, connectionConfig.get('filePath'), connectionConfig.get('encoding'))
      });
    }).catch(reject);
  });
};
