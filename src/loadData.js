var datawrap = require('datawrap');
var tools = require('./tools');

module.exports = function (source, tableData, database) {
  var tableName = tableData.name;
  var columns = tableData.columns;
  var data = tableData.data;

  return new datawrap.Bluebird(function (fulfill, reject) {
    var createTable = 'CREATE TABLE "' + tableName + '" (' + columns.map(function (column) {
      return '"' + column.name + '" ' + column.type.toUpperCase();
    }).join(', ') + ');';
    var insertStatement = 'INSERT INTO "' + tableName + '" (' + columns.map(function (column) {
      return '"' + column.name + '"';
    }).join(', ') + ') VALUES (' + columns.map(function (column) {
      return '{{' + column.name + '}}';
    }).join(', ') + ');';
    var objectData = data.map(function (row) {
      return Array.isArray(row) ? tools.addTitles(columns.map(function (d) {
        return d.name;
      }), row) : row;
    });

    var taskList = [{
      'name': 'Create Database',
      'task': database.runQuery,
      'params': [createTable]
    }, {
      'name': 'Insert Data',
      'task': database.runQuery,
      'params': [insertStatement, objectData, {
        'paramList': true
      }]
    }];

    datawrap.runList(taskList, 'Main Task')
      .then(function (a) {
        fulfill(a);
      }).catch(function (e) {
      reject(e);
    });
  });
};
