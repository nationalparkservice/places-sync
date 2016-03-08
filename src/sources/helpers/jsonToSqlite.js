var tools = require('../../tools');
var Promise = require('bluebird');
var database = require('../../databases');

var getColumnInfo = function (data, existingColumns, sourceConfig) {
  // Figure out what the columns are
  // Fill in any values that we didn't have before
  // If there defined columns, use only columns that aren in the defined list
  var newColumns = [];
  var defaultValues = {};
  // Try to read column info from the data, if data exists, otherwise use the existing columns
  if (data) {
    data.forEach(function (row) {
      for (var column in row) {
        var columnIndex = tools.simplifyArray(newColumns).indexOf(column) > -1 ? tools.simplifyArray(newColumns).indexOf(column) : newColumns.push({
          'name': column,
          'type': tools.getJsType(row[column])
        }) - 1;
        // Define the type we're going to use for SQLite
        newColumns[columnIndex].sqliteType = tools.getDataType(row[column], newColumns[columnIndex].sqliteType || 'integer', tools.getJsType(row[column]));
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
  } else {
    newColumns = JSON.parse(JSON.stringify(existingColumns));
  }

  // If there defined columns, use only columns that aren't in the defined list
  if (existingColumns) {
    newColumns = existingColumns.map(function (c) {
      var matchedColumn = newColumns.filter(function (nc) {
        return nc.name === c.name;
      })[0] || {};
      c.sqliteType = c.sqliteType || matchedColumn.sqliteType;
      c.defaultValue = c.defaultValue === undefined ? matchedColumn.defaultValue : c.defaultValue;
      return tools.denullify(c, true, [undefined]);
    });
  }

  return newColumns;
};

module.exports = function (data, existingColumns, sourceConfig) {
  return new Promise(function (fulfill, reject) {
    var columns = getColumnInfo(data, existingColumns, sourceConfig);

    var tempDbConfig = {
      'type': 'sqlite',
      'connection': ':memory:'
    };

    var createColumns = function (includeColumns) {
      var primaryKeys = includeColumns.filter(function (c) {
        return c.primaryKey;
      });

      var returnValue = '(' + includeColumns.map(function (column) {
        // console.log('c', column);
        var createColumn = '"' + column.name + '" ' + column.sqliteType;
        if (column.defaultValue !== undefined) {
          createColumn += " DEFAULT '" + column.defaultValue + "'";
        } else if (column.notNull === true) {
          createColumn += ' NOT NULL';
        }
        return createColumn;
      }).join(', ');
      if (sourceConfig.primaryKey && tools.arrayify(sourceConfig.primaryKey).length > 0) {
        returnValue += ', PRIMARY KEY (' + tools.surroundValues(tools.simplifyArray(primaryKeys), '"', '"').join(', ') + ')';
      }
      returnValue += ');';
      // console.log(returnValue);
      return returnValue;
    };

    // Create a 'CREATE TABLE' statement for the sqlite database
    var createStatementCache = 'CREATE TABLE "cached" ' + createColumns(columns);
    var createStatementUpdate = 'CREATE TABLE "updated" ' + createColumns(columns);
    var createStatementRemove = 'CREATE TABLE "removed" ' + createColumns(columns);

    // console.log(createStatementCache);
    // console.log(createStatementUpdate);
    // console.log(createStatementRemove);
    // console.log(createStatementKeyCache);
    // Create an 'INSERT INTO' statement for the sqlite database
    // TODO, use the createQueries tool
    var insertStatement = 'INSERT INTO "cached" (' + tools.simplifyArray(columns).map(function (c) {
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
      'task': '{{Create Database.query}}',
      'params': [createStatementCache]
    }, {
      'name': 'Create Updates Table',
      'task': '{{Create Database.query}}',
      'params': [createStatementUpdate]
    }, {
      'name': 'Create Removes Table',
      'task': '{{Create Database.query}}',
      'params': [createStatementRemove]
    }, {
      'name': 'Insert Data',
      'task': '{{Create Database.queryList}}',
      'params': [insertStatement, tools.arrayify(data)]
    }];
    tools.iterateTasks(taskList, 'jsonToSqlite', true).then(function (a) {
      fulfill(tools.arrayGetLast(a, true));
    }).catch(reject);
  });
};
