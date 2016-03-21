/* CartoDB File:
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = {
  'iterateTasks': require('../tools/iterateTasks'),
  'simplifyArray': require('../tools/simplifyArray')
};
var CreateQueries = require('./helpers/createQueries');
var columnsFromConfig = require('./helpers/columnsFromConfig');
var columnsToKeys = require('./helpers/columnsToKeys');
var databases = require('../databases');
var getSqliteType = require('./helpers/getSqliteTypeFromPg');

var QuerySource = function (connection, options, tableName, columns) {
  return function (type, whereObj, returnColumns) {
    returnColumns = returnColumns || columns;
    var keys = columnsToKeys(returnColumns);
    var createQueries = new CreateQueries(columns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField);
    return connection.query.apply(this, createQueries(type, whereObj, returnColumns, tableName));
  };
};

var WriteFn = function (connection, options, tableName, columns) {
  var keys = columnsToKeys(columns);
  var createQueries = new CreateQueries(columns, keys.primaryKeys);

  return function (updated, removed) {
    var tasks = [];
    var removeTasks = [];
    var keys = columnsToKeys(columns);
    if (keys.removedFieldValue !== undefined) {
      removed.forEach(function (row) {
        row[keys.removedField] = keys.removedFieldValue;
        updated.push(row);
      });
      removed = [];
    }
    updated.forEach(function (updatedRow, i) {
      tasks.push({
        'name': 'Remove / Write Update Row ' + i + JSON.stringify(updatedRow),
        'task': tools.iterateTasks,
        'params': [
          [{
            'name': 'Remove',
            'task': connection.query,
            'params': createQueries('remove', updatedRow, keys.primaryKeys, tableName)
          }, {
            'name': 'Write',
            'task': connection.query,
            'params': [createQueries('insert', undefined, columns, tableName)[0], updatedRow]
          }],
          'update cartodb db', true
        ]
      });
    });
    removed.forEach(function (removedRow, i) {
      removeTasks.push({
        'name': 'Remove Removed Row ' + i,
        'task': connection.query,
        'params': createQueries('remove', removedRow, keys.primaryKeys, tableName)
      });
    });

    return Promise.all(tasks.map(function (task) {
      return task.task.apply(this, task.params);
    })).then(function () {
      return Promise.all(removeTasks.map(function (removeTask) {
        return removeTask.task.apply(this, removeTask.params);
      }));
    });
  };
};

var readCartoDB = function (connection, options, tableName, tableSchema) {
  tableSchema = tableSchema || 'public';
  var queryObj = {
    'tableSchema': tableSchema,
    'tableName': tableName
  };
  var queries = [];

  // This will get us most of the info we need on the columns, but some types are (user defined)
  var query = '';
  query += 'SELECT';
  query += '  cdb_columnnames AS "name",';
  query += "  cdb_columntype('" + connection.account + '.' + tableName + "',";
  query += '  cdb_columnnames) AS "pgtype"';
  query += 'FROM';
  query += "  cdb_columnnames('";
  query += connection.account + '.' + tableName + "');";
  queries.push(query);

  // Since the user defined fields aren't available in columnNames, we run this query to get them
  queries.push('SELECT * FROM "' + connection.account + '"."' + tableName + '" LIMIT 0;');

  // To get if fields are nullable, and other info, we load this too
  query = '';
  query += 'SELECT ';
  query += '"column_name, "column_default", "is_nullable", "ordinal_position" ';
  query += "FROM CDB_TableIndexes('" + connection.account + '.' + tableName + "');";
  queries.push(query);

  // We need to run this to get the primary keys, since nothing else has them
  query = '';
  query += 'SELECT distinct ';
  query += 'unnest("index_keys") AS "name" ';
  query += 'FROM CDB_TableIndexes(\'' + connection.account + '.' + tableName +  '\') ';
  query += 'WHERE index_primary = true;'
  queries.push(query);

  return new Promise(function (fulfill, reject) {
    Promise.all(queries.map(function (q) {
      return connection.query(q, queryObj);
    })).then(function (result) {
      var columnNames = result[0] || [];
      var fields = result[1].fields || {};
      var columnInfoSchema = result[2] || [];
      var pkeys = tools.simplifyArray(result[3]);

      // Convert the SQLite column format into ours
      var columns = columnNames.map(function (column) {

        // Get types for when cartodb says "USER-DEFINED"
        var type = column.pgtype;
        if (type === 'USER-DEFINED' && fields[column.name] && fields[column.name].type) {
          type = fields[column.name].type;
        }

        // Get the row that matches from the infoSchema
        var matchedInfoSchema = columnInfoSchema[tools.simplifyArray(columnInfoSchema, 'column_name').indexOf(column.name)] || {};

        return {
          'name': column.name,
          'type': type,
          'sqliteType': getSqliteType(type),
          'primaryKey': pkeys.indexOf(column.name) > -1,
          'defaultValue': typeof matchedInfoSchema.column_default === 'object' ? undefined : matchedInfoSchema.column_default,
          'notNull': matchedInfoSchema.is_nullable === 'NO',
          'columnId': matchedInfoSchema.ordinal_position
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
    if (typeof (connectionConfig.get('table')) !== 'string') {
      throw new Error('A table name must be set in the connection for a SQLite file');
    }

    // Define the taskList
    var tasks = [{
      'name': 'createDbConnection',
      'description': 'Connects to the SQLite Database',
      'task': databases,
      'params': [{
        'type': 'cartodb',
        'connection': sourceConfig.connection
      }]
    }, {
      'name': 'convertFromTable',
      'description': 'Takes the source data and creates an update/remove cartodb table in memory for it',
      'task': readCartoDB,
      'params': ['{{createDbConnection}}', options, connectionConfig.get('table'), connectionConfig.get('schema')]
    }];
    tools.iterateTasks(tasks, 'cartodb').then(function (r) {
      var columns = columnsFromConfig(r.convertFromTable.columns, sourceConfig.fields);
      fulfill({
        'data': r[1].data,
        'columns': columns,
        'writeFn': new WriteFn(r[0], options, connectionConfig.get('table'), columns),
        'querySource': new QuerySource(r[0], options, connectionConfig.get('table'), columns)
      });
    }).catch(reject);
  });
};
