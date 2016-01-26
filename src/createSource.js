var md5 = require('./md5');
var Mockingbird = require('datawrap').mockingbird;
var tools = require('./tools');

module.exports = tools.syncPromise(function (source, database) {
  var tableName = source.name;
  var primaryKey = source.primaryKey || source.columns && source.columns[0].name || '1';
  primaryKey = tools.arrayify(primaryKey);
  var lastEditField = source.editFields && source.editFields.dateEdited;
  var columns = tools.simplifyArray(source.columns);

  return {
    'getRow': function (key, callback) {
      // If the table has a compund primary key, an array needs to be passed in here, or you won't get resuts back
      key = tools.arrayify(key);
      key = key.map(function (k) {
        return k.toString();
      });
      var sql = 'SELECT ';
      sql += '"' + columns.join('", "') + '" ';
      sql += 'FROM "' + tableName + '" ';
      sql += 'WHERE ';
      sql += '(' + primaryKey.map(function (k, i) {
          return '"' + k + '" = {{key.' + i + '}}';
        }).join(' AND ') + ')';
      sql += ';';
      return new (Mockingbird(callback))(function (fulfill, reject) {
        database.runQuery(sql, {
          'key': key
        }).then(function (res) {
          fulfill(res[0]);
        }).catch(function (e) {
          reject(tools.readError(e));
        });
      });
    },
    'getData': function (fromDate, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var sql = 'SELECT ' + columns.map(function (columnName) {
            return '"' + columnName + '"';
          }).join(', ') + ' FROM "' + tableName + '"';
        sql += (lastEditField && fromDate) ? ' WHERE "' + lastEditField + '" >= {{fromDate}};' : ';';
        database.runQuery(sql, {
          'fromDate': fromDate
        }).then(function (result) {
          fulfill(result[0]);
        }).catch(function (e) {
          reject(tools.readError(e));
        });
      });
    },
    'getHashedData': function (fromDate, callback) {
      var sql = 'SELECT ';
      sql += primaryKey.map(function (k, i) {
        return '"' + k + '"';
      }).join(', ') + ' ';
      sql += ', (' + columns.map(function (columnName) {
        return 'COALESCE(CAST("' + columnName + '" AS TEXT), \'\')';
      }).join(' || ') + ') AS "prehash" FROM "' + tableName + '"';
      sql += (lastEditField && fromDate) ? ' WHERE "' + lastEditField + '" >= {{fromDate}};' : ';';

      return new (Mockingbird(callback))(function (fulfill, reject) {
        database.runQuery(sql, {
          'fromDate': fromDate
        }).then(function (result) {
          var hashed = result[0].map(function (row) {
            var returnKey = primaryKey.map(function (k) {
              return row[k];
            });
            if (returnKey.length === 1) {
              returnKey = returnKey[0];
            }
            return {
              key: returnKey,
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
