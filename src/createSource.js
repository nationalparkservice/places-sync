var md5 = require('./md5');
var Mockingbird = require('datawrap').mockingbird;
var tools = require('./tools');
var createWhereClause = require('./createWhereClause');
var exportData = require('./exportData');

module.exports = tools.syncPromise(function (source, wrappedDatabase) {
  var tableName = source.name;
  var primaryKey = source.primaryKey || source.columns && source.columns[0].name || '1';
  primaryKey = tools.arrayify(primaryKey);
  var lastEditField = source.editInfo && source.editInfo.dateEdited;
  var columns = tools.simplifyArray(source.columns);

  var createSource = {
    'getRow': function (key, callback) {
      // If the table has a compound primary key, an array needs to be passed in here, or you won't get results back
      key = tools.arrayify(key);
      key = key.map(function (k) {
        return k.toString();
      });
      var primaryKeyQuery = {};
      primaryKey.forEach(function (k, i) {
        primaryKeyQuery[k] = key[i];
      });

      return new (Mockingbird(callback))(function (fulfill, reject) {
        return createSource.getDataWhere(primaryKeyQuery).then(function (r) {
          fulfill(Array.isArray(r) ? r[r.length - 1] : r);
        }).catch(reject);
      });
    },
    'getData': function (fromDate, callback) {
      var dateQuery;
      if (lastEditField && fromDate) {
        dateQuery = {};
        dateQuery[lastEditField] = {
          '$gte': fromDate
        };
      }
      return createSource.getDataWhere(dateQuery, callback);
    },
    'getDataWhere': function (whereObj, callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var sql = 'SELECT ';
        sql += columns.map(function (columnName) {
          return '"' + columnName + '"';
        }).join(', ');
        sql += ' FROM "';
        sql += tableName;
        sql += '"';
        var parsedWhereObj = whereObj ? createWhereClause(whereObj, columns) : [];
        sql += parsedWhereObj[0] ? ' WHERE ' + parsedWhereObj[0] + ';' : ';';
        wrappedDatabase._runQuery(sql, parsedWhereObj[1] || {}).then(function (result) {
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
      }).join(' || ') + ') AS "prehash"';
      sql += ', "' + lastEditField + '" as "lastEdited"';
      sql += ' FROM "' + tableName + '"';
      sql += (lastEditField && fromDate) ? ' WHERE "' + lastEditField + '" > {{fromDate}};' : ';';

      return new (Mockingbird(callback))(function (fulfill, reject) {
        if (columns.length === 0) {
          // No Data Here
          fulfill([]);
        } else {
          wrappedDatabase._runQuery(sql, {
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
                hash: md5(row.prehash),
                lastEdited: row.lastEdited
              };
            });
            fulfill(hashed);
          })
            .catch(function (e) {
              reject(tools.readError(e));
            });
        }
      });
    },
    'export': function (destination, where, remove, callback) {
      callback = typeof callback === 'function' ? callback : (typeof where === 'function' ? where : undefined);
      where = typeof where === 'object' ? where : undefined;
      return new (Mockingbird(callback))(function (fulfill, reject) {
        var exporter = exportData(destination, wrappedDatabase._config());
        exporter[(remove ? 'remove' : 'add') + 'Data']({
          source: createSource,
          where: where
        }).then(fulfill, reject);
      });
    },
    '_source': function () {
      var tmpSource = {};
      for (var field in source) {
        if (field !== 'data') {
          tmpSource[field] = JSON.parse(JSON.stringify(source[field]));
        }
      }
      return tmpSource;
    },
    'name': source.name
  };
  return source.columns ? createSource : {};
});
