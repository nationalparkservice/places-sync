/* JSON File:
 *   *filePath: (path the the json file)
 *   encoding: (defaults to 'UTF8')
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = require('../tools');
var jsonToSqlite = require('./helpers/jsonToSqlite');

var DatabaseObject = function (connection, tableName, columns) {
  return {
    runQuery: function (sql, params) {}
  };
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
      'task': tools.readFile,
      'params': [connectionConfig.get('filePath'), connectionConfig.get('encoding')]
    }, {
      'name': 'createDatabaseFromJson',
      'description': 'Creates a sqlite database from Json data',
      'task': jsonToSqlite,
      'params': ['{{openFile.data}}']
    }];
    tools.iterateTasks(tasks).then(function (r) {
      var connection = r[1];
      var tableName = sourceConfig.name;
      var columns = r[1].columns.map(function (column) {
        column.primaryKey = tools.arrayify(sourceConfig.primaryKey).indexOf(column.name) !== -1;
        column.lastUpdated = tools.arrayify(sourceConfig.lastUpdated).indexOf(column.name) !== -1;
      });
      fulfill(new DatabaseObject(connection, tableName, columns));
    }).catch(function (e) {
      reject(Array.isArray(e) ? e[e.length - 1] : e);
    });
  });
};
