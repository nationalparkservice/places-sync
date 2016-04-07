/* Postgresql File:
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = {
  'iterateTasks': require('../tools/iterateTasks'),
  'simplifyArray': require('../tools/simplifyArray')
};
var databases = require('../databases');
var CreateQueries = require('./helpers/createQueries');
var columnsFromConfig = require('./helpers/columnsFromConfig');
var getSqliteType = require('./helpers/getSqliteTypeFromPg');
var columnsToKeys = require('./helpers/columnsToKeys');

var QuerySource = function (connection, options, tableName, columns) {
  return function (type, whereObj, returnColumns) {
    returnColumns = returnColumns || columns;
    var keys = columnsToKeys(returnColumns);
    var createQueries = new CreateQueries(columns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField);

    // Casts!
    var preQuery = createQueries(type, whereObj, returnColumns, tableName);
    console.log('@$%^#$^%#%^');
    console.log(preQuery);
    console.log('@$%^#$^%#%^');

    return connection.query.apply(this, preQuery);
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
          'update postgresql db', true
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

var readPostgresql = function (connection, options, tableName, tableSchema) {
  tableSchema = tableSchema || 'public';
  var queryObj = {
    'tableSchema': tableSchema,
    'tableName': tableName
  };
  return new Promise(function (fulfill, reject) {
    Promise.all([
      connection.query('SELECT pg_attribute.attname AS "key" FROM pg_index, pg_class, pg_attribute, pg_namespace WHERE   pg_class.oid = {{tableName}}::regclass AND   indrelid = pg_class.oid AND   nspname = {{tableSchema}} AND   pg_class.relnamespace = pg_namespace.oid AND   pg_attribute.attrelid = pg_class.oid AND   pg_attribute.attnum = any(pg_index.indkey) AND indisprimary;', queryObj),
      connection.query('SELECT * FROM information_schema.columns WHERE table_schema = {{tableSchema}} AND table_name = {{tableName}};', queryObj)
    ]).then(function (result) {
      var rawColumns = result[1];
      var pkeys = tools.simplifyArray(result[0].key);
      // TODO: allow an option to pull the data into the source, like in csvs
      var data;
      var columns = rawColumns.map(function (rawColumn) {
        return {
          'name': rawColumn.column_name,
          'type': rawColumn.data_type,
          'sqliteType': getSqliteType(rawColumn.data_type),
          'primaryKey': pkeys.indexOf(rawColumn.name) > -1, // http://stackoverflow.com/questions/1214576/how-do-i-get-the-primary-keys-of-a-table-from-postgres-via-plpgsql
          'defaultValue': typeof rawColumn.column_default=== 'object' ? undefined : rawColumn.column_default,
          'notNull': rawColumn.is_nullable === 'NO',
          'columnId': rawColumn.ordinal_position
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
    if (typeof (connectionConfig.get('table')) !== 'string') {
      throw new Error('A table name must be set in the connection for a PostgreSQL connection');
    }

    // Define the taskList
    var tasks = [{
      'name': 'createDbConnection',
      'description': 'Connects to the PostgreSQL Database',
      'task': databases,
      'params': [{
        'type': 'postgresql',
        'connection': sourceConfig.connection
      }]
    }, {
      'name': 'convertFromTable',
      'description': 'Takes the source data and creates an update/remove postgresql table in memory for it',
      'task': readPostgresql,
      'params': ['{{createDbConnection}}', options, connectionConfig.get('table'), connectionConfig.get('schema')]
    }];
    tools.iterateTasks(tasks, 'postgresql').then(function (r) {
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
