/* CartoDB File:
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = {
  'iterateTasks': require('../tools/iterateTasks'),
  'simplifyArray': require('../tools/simplifyArray'),
  'setProperty': require('../tools/setProperty'),
  'dummyPromise': require('../tools/dummyPromise')
};
var CreateQueries = require('./helpers/createQueries');
var columnsFromConfig = require('./helpers/columnsFromConfig');
var columnsToKeys = require('./helpers/columnsToKeys');
var mapColumns = require('./helpers/mapColumns');
var databases = require('../databases');
var getSqliteType = require('./helpers/getSqliteTypeFromPg');

var QuerySource = function (connection, options, account, tableName, columns) {
  return function (type, whereObj, returnColumns) {
    returnColumns = returnColumns || columns;
    var unmappedColumns = mapColumns.from(columns, unmappedColumns);
    var keys = columnsToKeys(unmappedColumns);
    var createQueries = new CreateQueries(unmappedColumns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField, options);
    // TODO createQueries should take a schema
    return connection.query.apply(this, createQueries(type, whereObj, returnColumns, tableName).concat([false, unmappedColumns]));
  };
};

var WriteFn = function (connection, options, account, tableName, columns) {
  var unmappedColumns = mapColumns.from(columns, unmappedColumns);
  var keys = columnsToKeys(unmappedColumns);
  var createQueries = new CreateQueries(unmappedColumns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField, options);

  // TODO: put this somewhere better
  if (options && options.transforms) {
    for (var transform in options.transforms) {
      var columnIdx = tools.simplifyArray(unmappedColumns).indexOf(transform);
      if (columnIdx > -1) {
        unmappedColumns[columnIdx].transformed = true;
      }
    }
  }

  return function (updated, removed) {
    var tasks = [];
    var removeTasks = [];
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
            'params': createQueries('remove', updatedRow, keys.primaryKeys, tableName).concat([false, unmappedColumns])
          }, {
            'name': 'Write',
            'task': connection.query,
            'params': [createQueries('insert', undefined, unmappedColumns, tableName)[0], updatedRow, false, unmappedColumns]
          }],
          'update cartodb db', true
        ]
      });
    });
    removed.forEach(function (removedRow, i) {
      removeTasks.push({
        'name': 'Remove Removed Row ' + i,
        'task': connection.query,
        'params': createQueries('remove', removedRow, keys.primaryKeys, tableName).concat([false, unmappedColumns])
      });
    });

    return Promise.all(tasks.map(function (task) {
      return task.task.apply(this, task.params);
    })).then(function () {
      return Promise.all(removeTasks.map(function (removeTask) {
        return removeTask.task.apply(this, removeTask.params).then(function () {
          return tools.dummyPromise({
            'updated': updated,
            'removed': removed
          });
        });
      }));
    });
  };
};

var readCartoDB = function (connection, options, config) {
  var queryObj = {
    'tableSchema': config.account,
    'tableName': config.tableName
  };
  var queries = [];

  // This will get us most of the info we need on the columns, but some types are (user defined)
  var query = '';
  query += 'SELECT';
  query += '  cdb_columnnames AS "name",';
  query += "  cdb_columntype('" + config.account + '.' + config.tableName + "',";
  query += '  cdb_columnnames) AS "pgtype"';
  query += 'FROM';
  query += "  cdb_columnnames('";
  query += config.account + '.' + config.tableName + "');";
  queries.push(query);

  // Since the user defined fields aren't available in columnNames, we run this query to get them
  queries.push('SELECT * FROM "' + config.account + '"."' + config.tableName + '" LIMIT 0;');

  // To get if fields are nullable, and other info, we load this too
  query = '';
  query += 'SELECT ';
  query += '"column_name", "column_default", "is_nullable", "ordinal_position", "data_type" ';
  query += 'FROM "information_schema"."columns" ';
  query += 'WHERE "table_schema" = \'' + config.account + '\' AND "table_name" = \'' + config.tableName + "' ";
  query += 'ORDER BY "ordinal_position";';
  queries.push(query);

  // We need to run this to get the primary keys, since nothing else has them
  query = '';
  query += 'SELECT distinct ';
  query += 'unnest("index_keys") AS "name" ';
  query += "FROM CDB_TableIndexes('" + config.account + '.' + config.tableName + "') ";
  query += 'WHERE index_primary = true;';
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
          'defaultValue': undefined, // typeof matchedInfoSchema.column_default === 'object' ? undefined : matchedInfoSchema.column_default,
          'notNull': matchedInfoSchema.is_nullable === 'NO',
          'columnId': matchedInfoSchema.ordinal_position
        };
      });

      fulfill({
        'data': undefined,
        'columns': columns
      });
    }).catch(reject);
  });
};

module.exports = function (sourceConfig, options) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    var requirements = ['account', 'apiKey', 'tableName'];
    requirements.forEach(function (requirement) {
      if (typeof connectionConfig.get(requirement) !== 'string') {
        throw new Error(requirement + ' must be defined for a CartoDB connection');
      }
    });
    options = options || {};
    options.transforms = options.transforms || {};

    // Define the taskList
    var tasks = [{
      'name': 'createDbConnection',
      'description': 'Connects to the SQLite Database',
      'task': databases,
      'params': [{
        'type': 'cartodb',
        'connection': connectionConfig.toObject()
      }]
    }, {
      'name': 'convertFromTable',
      'description': 'Takes the source data and creates an update/remove cartodb table in memory for it',
      'task': readCartoDB,
      'params': ['{{createDbConnection}}', options, connectionConfig.toObject()]
    }];
    tools.iterateTasks(tasks, 'cartodb').then(function (r) {
      var columns = columnsFromConfig(r.convertFromTable.columns, sourceConfig.fields);
      var mappedColumns = mapColumns.to(columns);
      var keys = columnsToKeys(columns);
      options.transforms[keys.lastUpdatedField] = options.transforms[keys.lastUpdatedField] || {
        'from': ['(EXTRACT(EPOCH FROM ', ')) * 1000'],
        'to': ["(TIMESTAMP 'epoch' + ", " * INTERVAL '1 millisecond') AT TIME ZONE 'GMT'"]
      };

      fulfill({
        'data': r[1].data,
        'columns': mappedColumns,
        'writeFn': new WriteFn(r[0], options, connectionConfig.get('account'), connectionConfig.get('tableName'), mappedColumns),
        'querySource': new QuerySource(r[0], options, connectionConfig.get('account'), connectionConfig.get('tableName'), mappedColumns)
      });
    }).catch(reject);
  });
};
