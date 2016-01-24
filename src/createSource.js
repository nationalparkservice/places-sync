var md5 = require('./md5');
var Mockingbird = require('datawrap').mockingbird;
var tools = require('./tools');

module.exports = tools.syncPromise(function (source, database) {
  var tableName = source.name;
  var primaryKey = source.primaryKey || source.columns && source.columns[0].name || '1';
  var lastEditField = source.editFields && source.editFields.dateEdited;
  var columns = tools.simplifyArray(source.columns);

  return {
    'getRowAt': function (key, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var sql = 'SELECT "' + primaryKey + '", "' + columns.join('", "') + '" FROM "' + tableName + '" WHERE "' + primaryKey + '" = {{key}};';
        database.runQuery(sql, {
          'key': key.toString()
        }).then(function (res) {
          console.log('sql', sql, key.toString());
          console.log('result', res[0]);
        }).catch(reject);
      });
    },
    'getHashedData': function (fromDate, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var sql = 'SELECT "' + primaryKey + '" AS key, (' + columns.map(function (columnName) {
          return 'COALESCE(CAST("' + columnName + '" AS TEXT), \'\')';
        }).join(' || ') + ') AS "prehash" FROM "' + tableName + '"';
        sql += (lastEditField && fromDate) ? ' WHERE "' + lastEditField + '" >= {{fromDate}};' : ';';
        console.log('sql', sql);
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
            reject(e[e.length - 1]);
          });
      });
    },
    'runQuery': function (query, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        database.runQuery(query).then(fulfill).catch(function (e) {
          reject(e[e.length - 1]);
        });
      });
    },
    'close': function (callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var command = [null, null, {
          'close': true
        }];
        database.runQuery.apply(database, command).then(fulfill).catch(function (e) {
          reject(e[e.length - 1]);
        });
      });
    },
    'name': source.name
  };
});
