var md5 = require('./md5');
var Mockingbird = require('datawrap').mockingbird;
var tools = require('./tools');

module.exports = tools.syncPromise(function (source, database) {
  var tableName = source.name;
  var columns = {
    'primaryKey': source.primaryKey || source.columns && source.columns[0].name || '1',
    'lastUpdated': source.lastUpdated,
    'columns': (source.columns || []).map(function (column) {
      return column.name;
    })
  };
  console.log(columns);

  return {
    'getRowAt': function (key, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var sql = 'SELECT "' + columns.primaryKey + '", "' + columns.columns.join('", "') + '" FROM "' + tableName + '" WHERE "' + columns.primaryKey + '" = {{key}};';
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
        var sql = 'SELECT "' + columns.primaryKey + '" AS key, (' + columns.columns.map(function (columnName) {
            return 'COALESCE(CAST("' + columnName + '" AS TEXT), \'\')';
          }).join(' || ') + ') AS "prehash" FROM "' + tableName + '"';
        sql += (columns.lastUpdated && fromDate) ? ' WHERE "' + columns.lastUpdated + '" >= {{fromDate}};' : ';';
        console.log('sql', sql);
        database.runQuery(sql, {
          'fromDate': fromDate
        })
          .then(function (result) {
            var hashed = result[0].map(function (row) {
              return {
                key: row.key,
                hash: md5(row.prehash)
              };
            });
            fulfill(hashed);
          })
          .catch(reject);
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
