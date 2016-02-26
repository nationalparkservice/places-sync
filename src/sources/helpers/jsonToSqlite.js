var tools = require('../../tools');
var Promise = require('bluebird');
var database = require('../../databases');

var getColumnInfo = function (data) {
  // Figure out what the columns are
  var columns = [];
  data.forEach(function (row) {
    for (var column in row) {
      var columnIndex = tools.simplifyArray(columns).indexOf(column) > -1 ? tools.simplifyArray(columns).indexOf(column) : columns.push({
        'name': column,
        'nativeType': tools.getJsType(row[column])
      }) - 1;
      column[columnIndex].sqliteType = tools.getDataType(row[column], column[columnIndex].sqliteType || 'integer', column[columnIndex].nativeType);
    }
  });
  return columns;
};

var wrapFn = function (fn, args) {
  return fn.apply(this, args);
};

module.exports = function (data, columns, sourceConfig) {
  return new Promise(function (fulfill, reject) {
    columns = columns || getColumnInfo(data);

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
