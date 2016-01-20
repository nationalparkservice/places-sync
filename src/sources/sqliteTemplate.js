var md5 = require('../md5');
var Bluebird = require('datawrap').Bluebird;

module.exports = function (tableName, columns, db) {
  return {
    'getRowAt': function (key) {
      return new Bluebird(function (fulfill, reject) {
        var sql = 'SELECT "' + columns.primaryKey + '", "' + columns.columns.join('", "') + '" FROM "' + tableName + '" WHERE "' + columns.primaryKey + '" = {{key}};';
        db.runQuery(sql, {
          'key': key.toString()
        }).then(function (res) {
          console.log('sql', sql, key.toString());
          console.log('result', res[0]);
        }).catch(reject);
      });
    },
    'getHashedData': function (fromDate) {
      return new Bluebird(function (fulfill, reject) {
        var sql = 'SELECT "' + columns.primaryKey + '" AS key, (' + columns.columns.map(function (columnName) {
          return 'COALESCE(CAST("' + columnName + '" AS TEXT), \'\')';
        }).join(' || ') + ') AS "prehash" FROM "' + tableName + '"';
        sql += (columns.lastUpdated && fromDate) ? ' WHERE "' + columns.lastUpdated + '" >= {{fromDate}};' : ';';
        console.log('sql', sql);
        db.runQuery(sql, {
          'fromDate': fromDate
        })
          .then(function (result) {
            var hashed = result[0].map(function (row) {
              return {
                key: row.key,
                hash: md5(row.prehash)
              };
            });
            console.log('sql', sql);
            console.log('result', hashed);
          })
          .catch(reject);
      });
    },
    'close': function () {
      return new Bluebird(function (fulfill, reject) {
        var command = [null, null, {
          'close': true
        }];
        db.runQuery.apply(db, command).then(fulfill).catch(reject);
      });
    }
  };
};
