var datawrap = require('datawrap');
var tools = require('./tools');

module.exports = function (source, wrappedDatabase) {
  var tableName = source.name;
  var columns = source.columns;
  var data = source.data;

  if (data.length === 0 && columns.length === 0) {
    return tools.syncPromise(function (value) {
      return value;
    })(wrappedDatabase);
  }
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
    console.log('a', createTable);
    console.log('b', insertStatement);
    console.log('c', objectData);
    console.log('d', source.data);

    var taskList = [{
      'name': 'Create Database',
      'task': wrappedDatabase.database.runQuery,
      'params': [createTable]
    }, {
      'name': 'Insert Data',
      'task': wrappedDatabase.database.runQuery,
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
