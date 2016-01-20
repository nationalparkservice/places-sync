/* columns must contain the fields:
 *   primaryKey: primary key for the table (TODO: Support compound keys)
 *   columns: an array of all other columns to be updated
 *
 * columns can also contain the fields
 *   lastUpdated: field name for the last update (default UTC time, TODO: support more time options)
 *   removed: field name for the field that denotes if the record still exists (true means removed, TODO: support false as well)
 *
 */
var guid = require('./guid');
var md5 = require('./md5');
var datawrap = require('datawrap');
var csvDb = require('./csvDb');
var Bluebird = datawrap.Bluebird;
var createSource = module.exports = function (sourceId, type) {
  var types = {
    'csv': function (file, columns, customFunctions) {
      return new Bluebird(function (fulfill, reject) {
        // Create Datasource for this csv file
        var tableName = guid();
        var config = {
          'name': tableName,
          'data': {}
        };
        var defaults = {
          'fileDesignator': 'file:///',
          'fileOptions': {
            'encoding': 'utf8'
          },
          'csvDirectory': __dirname,
          'rootDirectory': __dirname
        };

        config.data[tableName] = file;
        csvDb(config, defaults)
          .then(function (csvObj) {
            var db = csvObj.database;
            fulfill({
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
            });
          }).catch(reject);
      });
    }
  };
  var returnValue = types[type];
  returnValue.id = sourceId;
  return returnValue;
};


