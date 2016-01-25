var md5 = require('./md5');
var Mockingbird = require('datawrap').mockingbird;
var tools = require('./tools');

module.exports = tools.syncPromise(function (source, database) {
  var tableName = source.name;
  var primaryKey = source.primaryKey || source.columns && source.columns[0].name || '1';
  var lastEditField = source.editFields && source.editFields.dateEdited;
  var columns = tools.simplifyArray(source.columns);

  return {
    'getRow': function (key, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var sql = 'SELECT "' + primaryKey + '", "' + columns.join('", "') + '" FROM "' + tableName + '" WHERE "' + primaryKey + '" = {{key}};';
        database.runQuery(sql, {
          'key': key.toString()
        }).then(function (res[0]) {
          fulfill(res);
        }).catch(function (e) {
          reject(tools.readError(e));
        });
      });
    },
    'getHashedData': function (fromDate, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var sql = 'SELECT "' + primaryKey + '" AS key, (' + columns.map(function (columnName) {
          return 'COALESCE(CAST("' + columnName + '" AS TEXT), \'\')';
        }).join(' || ') + ') AS "prehash" FROM "' + tableName + '"';
        sql += (lastEditField && fromDate) ? ' WHERE "' + lastEditField + '" >= {{fromDate}};' : ';';
        // console.log('sql', sql);
        database.runQuery(sql, {
          'fromDate': fromDate
        }).then(function (result) {
          var hashed = result[0].map(function (row) {
            return {
              key: row.key,
              hash: md5(row.prehash)
            };
          });
          fulfill(hashed);
        })
          .catch(function (e) {
            reject(tools.readError(e));
          });
      });
    },
    '_source': function () {
      var tmpSource = JSON.parse(JSON.stringify(source));
      delete tmpSource.data;
      return tmpSource;
    },
    'name': source.name
  };
});
