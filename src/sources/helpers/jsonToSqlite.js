var tools = require('../../tools');
var Promise = require('bluebird');
var database = require('../../databases');

var getColumnInfo = function (data, existingColumns) {
  // Figure out what the columns are
  // Fill in any values that we didn't have before
  // If there defined columns, use only columns that aren in the defined list
  var newColumns = [];
  var defaultValues = {};
  data.forEach(function (row) {
    for (var column in row) {
      var columnIndex = tools.simplifyArray(newColumns).indexOf(column) > -1 ? tools.simplifyArray(newColumns).indexOf(column) : newColumns.push({
        'name': column,
        'nativeType': tools.getJsType(row[column])
      }) - 1;
      // Define the type we're going to use for SQLite
      newColumns[columnIndex].sqliteType = tools.getDataType(row[column], newColumns[columnIndex].sqliteType || 'integer', newColumns[columnIndex].nativeType);
      // If every value in the table for a single column is the same, we'll assume that to be the default
      if (defaultValues[column]) {
        defaultValues[column].hasDefault = defaultValues[column].hasDefault && defaultValues[column].firstValue === row[column];
      } else {
        defaultValues[column] = {
          'hasDefault': true,
          'firstValue': row[column]
        };
      }
    }
  });

  // Add in the default values if there are any
  newColumns = newColumns.map(function (c) {
    if (defaultValues[c.name].hasDefault === true) {
      c.defaultValue = defaultValues[c.name].firstValue;
    }
    return c;
  });

  // If there defined columns, use only columns that aren in the defined list
  if (existingColumns) {
    newColumns = existingColumns.map(function (c) {
      var matchedColumn = newColumns.filter(function (nc) {
        return nc.name === c.name;
      })[0] || {};
      c.sqliteType = c.sqliteType || matchedColumn.sqliteType;
      c.defaultValue = c.defaultValue || matchedColumn.defaultValue;
      return tools.denullify(c, true, [undefined]);
    });
  }
  return newColumns;
};

var wrapFn = function (fn, args) {
  return fn.apply(this, args);
};

module.exports = function (data, existingColumns, sourceConfig) {
  return new Promise(function (fulfill, reject) {
    var columns = getColumnInfo(data, existingColumns);

    var tempDbConfig = {
      'type': 'sqlite',
      'connection': ':memory:'
    };

    // Create a 'CREATE TABLE' statement for the sqlite database
    var createColumns = '(' + columns.map(function (column) {
      return '"' + column.name + '" ' + column.sqliteType;
    }).join(', ');
    if (sourceConfig.primaryKey && tools.arrayify(sourceConfig.primaryKey).length > 0) {
      createColumns += 'PRIMAY KEY (' + tools.arrayify(sourceConfig.primaryKey).join(', ') + ')';
    }
    createColumns += ');';
    var createStatementData = 'CREATE TABLE source ' + createColumns;
    var createStatementUpdate = 'CREATE TABLE new ' + createColumns;
    var createStatementRemove = 'CREATE TABLE remove ' + createColumns;

    // Create an 'INSERT INTO' statement for the sqlite database
    var insertStatement = 'INSERT INTO source (' + tools.simplifyArray(columns).map(function (c) {
      return '"' + c + '"';
    }).join(', ') + ') VALUES (' + tools.simplifyArray(columns).map(function (c) {
      return '{{' + c + '}}';
    }).join(', ') + ')';

    var taskList = [{
      'name': 'Create Database',
      'description': 'Create a sqlite database in memory',
      'task': database,
      'params': [tempDbConfig]
    }, {
      'name': 'Create Data Table',
      'task': wrapFn,
      'params': ['{{Create Database.query}}', [createStatementData]]
    }, {
      'name': 'Create Updates Table',
      'task': wrapFn,
      'params': ['{{Create Database.query}}', [createStatementUpdate]]
    }, {
      'name': 'Create Removes Table',
      'task': wrapFn,
      'params': ['{{Create Database.query}}', [createStatementRemove]]
    }, {
      'name': 'Insert Data',
      'task': wrapFn,
      'params': ['{{Create Database.queryList}}', [insertStatement, data]]
    }, {
      'name': 'test',
      task: wrapFn,
      'params': ['{{Create Database.query}}', ['select * from source;']]
    }];
    tools.iterateTasks(taskList, 'jsonToSqlite').then(function (a) {
      fulfill(tools.arrayGetLast(a, true));
    }).catch(function (e) {
      reject(tools.arrayGetLast(e));
    });
  });
};
