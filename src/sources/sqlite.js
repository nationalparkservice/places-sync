/* SQLite File:
 *   *connection.filePath: (path to the sqlite file)
 *   *tableName, defaults to name if this isn't present
 *   *primaryKey is read from the sqlite database, if one is supplied it will use the supplied one instead
 *   *  using a different primary key may cause unexpected issues (with null and unique contraints)
 *   *  so pay attention to this is you're using custom primary keys
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = {
  'iterateTasks': require('../tools/iterateTasks'),
  'arrayify': require('../tools/arrayify'),
  'updateObject': require('../tools/updateObject')
};
var databases = require('../databases');
var CreateQueries = require('./helpers/createQueries');

var QuerySource = function (connection, options, tableName, columns) {
  return function (type, whereObj, returnColumns) {
    returnColumns = returnColumns || columns;
    var primaryKeys = returnColumns.filter(function (c) {
      return c.primaryKey;
    });
    var lastUpdatedField = returnColumns.filter(function (c) {
      return c.primaryKey;
    })[0];
    var createQueries = new CreateQueries(columns, primaryKeys, lastUpdatedField);
    return createQueries(type, whereObj, returnColumns, tableName);
  };
};

var WriteFn = function (connection, options, tableName, columns) {
  var primaryKeys = columns.filter(function (c) {
    return c.primaryKey;
  });
  var createQueries = new CreateQueries(columns, primaryKeys);

  return function (updated, removed) {
    return new Promise(function (fulfill, reject) {
      var tasks = [];
      updated.forEach(function (updatedRow, i) {
        tasks.push({
          'name': 'Remove Update Row ' + i,
          'task': connection.query,
          'params': createQueries('remove', updatedRow, primaryKeys, tableName)
        });
        tasks.push({
          'name': 'Write Update Row ' + i,
          'task': connection.query,
          'params': [createQueries('insert', undefined, columns, tableName)[0], updatedRow]
        });
      });
      removed.forEach(function (removedRow, i) {
        tasks.push({
          'name': 'Remove Removed Row ' + i,
          'task': connection.query.apply,
          'params': [this, createQueries('remove', removedRow, primaryKeys, tableName)]
        });
      });

      tools.iterateTasks(tasks, 'update sqlite db', true).then(fulfill).catch(reject);
    });
  };
};

var readSqlite = function (connection, options, tableName) {
  return new Promise(function (fulfill, reject) {
    connection.query("PRAGMA table_info('" + tableName + "');").then(function (rawColumns) {
      // TODO: allow an option to pull the data into the source, like in csvs
      var data;
      // Convert the SQLite column format into ours
      var columns = rawColumns.map(function (rawColumn) {
        return {
          'name': rawColumn.name,
          'type': rawColumn.type,
          'sqliteType': rawColumn.type,
          'primaryKey': rawColumn.pk !== 0,
          'defaultValue': typeof rawColumn.dflt_value === 'object' ? undefined : rawColumn.dflt_value,
          'notNull': rawColumn.notnull === 1,
          'sqliteColumnId': rawColumn.cid
        };
      });
      fulfill({
        'data': data,
        'columns': columns
      });
    }).catch(reject);
  });
};

module.exports = function (sourceConfig, options) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    if (typeof connectionConfig.get('filePath') !== 'string') {
      throw new Error('filePath must be defined for a SQLite file');
    }
    if (typeof (sourceConfig.tableName || sourceConfig.name) !== 'string') {
      throw new Error('tableName or name must be defined for a SQLite file');
    }

    // Define the taskList
    var tasks = [{
      'name': 'createDbConnection',
      'description': 'Connects to the SQLite Database',
      'task': databases,
      'params': [{
        'type': 'sqlite',
        'connection': connectionConfig.get('filePath')
      }]
    }, {
      'name': 'convertFromTable',
      'description': 'Takes the source data and creates an update/remove sqlite table in memory for it',
      'task': readSqlite,
      'params': ['{{createDbConnection}}', options, (sourceConfig.tableName || sourceConfig.name)]
    }];
    tools.iterateTasks(tasks, 'sqlite').then(function (r) {
      var columns = r[1].columns.map(function (column) {
        column.primaryKey = sourceConfig.primaryKey ? tools.arrayify(sourceConfig.primaryKey).indexOf(column.name) !== -1 : column.primaryKey;
        column.lastUpdatedField = tools.arrayify(sourceConfig.lastUpdatedField).indexOf(column.name) !== -1;
        column.removedField = tools.arrayify(sourceConfig.removedField).indexOf(column.name) !== -1;
        return column;
      });
      fulfill({
        'data': r[1].data,
        'columns': columns,
        'writeFn': new WriteFn(r[0], options, sourceConfig.tableName || sourceConfig.name, columns),
        'querySource': new QuerySource(r[0], options, (sourceConfig.tableName || sourceConfig.name), columns)
      });
    }).catch(reject);
  });
};
