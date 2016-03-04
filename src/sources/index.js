// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
// var sources = tools.requireDirectory(__dirname, [__filename]);
var createDatabase = require('./helpers/jsonToSqlite');
var CreateQueries = require('./helpers/createQueries');

var promiseError = function (msg) {
  return new Promise(function (f, r) {
    r(new Error(msg));
  });
};

var verifyKeys = function (row, keys) {
  return new Promise(function (fulfill, reject) {
    // console.log('verify', row, keys);
    keys = tools.arrayify(keys);
    var keyCount = 0;
    for (var column in row) {
      keyCount += keys.indexOf(column) > -1 ? 1 : 0;
    }
    if (keys.length !== keyCount) {
      reject(new Error('All keys must be present in a query \nkeys: ' + JSON.stringify(keys) + '\nYour Query: ' + JSON.stringify(row)));
    } else {
      fulfill(keys.length === keyCount);
    }
  });
};

var getDefaults = function (row, columns) {
  var newRow = {};
  columns.forEach(function (column) {
    newRow[column.name] = row[column.name] !== undefined ? row[column.name] : (column.defaultValue === undefined ? null : column.defaultValue);
  });
  return newRow;
};

var getPrimaryKeysOnly = function (primaryKeys, row) {
  var rowPrimaryKeys = {};
  primaryKeys.forEach(function (pk) {
    rowPrimaryKeys[pk] = row[pk];
  });
  return rowPrimaryKeys;
};

var SourceObject = function (database, columns, writeToSource, querySource, masterCache) {
  // Query based on the primary key(s), or all keys if no primary keys exist
  var primaryKeys = tools.simplifyArray(columns.filter(function (c) {
    return c.primaryKey;
  }));
  var lastUpdatedField = tools.simplifyArray(columns.filter(function (c) {
    return c.lastUpdatedField;
  }))[0];
  var createQueries = new CreateQueries(columns, primaryKeys, lastUpdatedField);

  var updateRow = function (row, remove) {
    var updatedRowData = [];
    return verifyKeys(row, primaryKeys)
      .then(function () {
        return actions.cache.selectAll(getPrimaryKeysOnly(primaryKeys, row));
      })
      .then(function (results) {
        return Promise.all(results.map(function (resultRow) {
          updatedRowData.push(tools.updateObject(resultRow, row));
          // console.log('updatedRowData', updatedRowData[updatedRowData.length - 1]);
          // console.log('resultRow', resultRow);
          // console.log('row', row);
          // Remove any entries for this row in the removes table
          // console.log('cleanRemove', createQueries('cleanRemove', resultRow, columns));
          return database.query.apply(this, createQueries('cleanRemove', resultRow, columns))
            .then(function () {
              // Remove any entries for this row in the Updates table
              return database.query.apply(this, createQueries('cleanUpdate', resultRow, columns));
            });
        })).then(function (res) {
          // Insert this into the update or remove table
          // console.log('res?', res, remove, updatedRowData, getDefaults(row, columns));
          var query = createQueries('run' + (remove ? 'Remove' : 'Update'))[0];
          var completeRow = updatedRowData.length === 1 ? updatedRowData[0] : getDefaults(row, columns);
          // console.log('insert' + (remove ? 'Remove' : 'Update'), query, completeRow);
          return database.query(query, completeRow);
        });
      });
  };

  var actions = {
    'cache': {
      'selectAll': function (rowData) {
        // Selects all fields, if not rowData is supplied, it will query everything
        return database.query.apply(this, createQueries('selectAllInCache', rowData, columns));
      }
    },
    'selectAllKeys': function () {
      // This gets ALL keys from the source, including ones that we didn't pull down to our database
      return new Promise(function (fulfill, reject) {
        var tasks = [
          database.query.apply(this, createQueries('selectAllKeys', primaryKeys))
        ];
        Promise.all(tasks).then(function (keys) {
          var allKeys = [];
          keys.forEach(function (keyList) {
            keyList.forEach(function (key) {
              var txtKey = JSON.stringify(key);
              if (allKeys.indexOf(txtKey) === -1) {
                allKeys.push(txtKey);
              }
            });
          });
          fulfill(allKeys.map(function (txtKey) {
            return JSON.parse(txtKey);
          }));
        }).catch(reject);
      });
    },
    'selectLastUpdate': function (rowData) {
      // gets the max date from the lastUpdate field, a rowData can be applied to this
      return new Promise(function (fulfill, reject) {
        if (querySource) {
          querySource('selectLastUpdate', rowData, 'lastUpdate').then(function (r) {
            fulfill(r[0].lastUpdate);
          }).catch(reject);
        } else {
          database.query.apply(this, createQueries('selectLastUpdate', rowData, undefined, 'cached')).then(function (r) {
            fulfill(r[0].lastUpdate);
          }).catch(reject);
        }
      });
    },
    'get': {
      'columns': function () {
        // Returns the columns and their data types
        // as well as what the primaryKey(s) is/are and the lastEdit field
        return JSON.parse(JSON.stringify(columns));
      },
      'selectAll': function (rowData, type) {
        type = type !== 'selectAll' ? 'select' : type; // Prevent anything weird from getting passed in
        if (querySource) {
          return querySource(type, rowData, columns);
        } else {
          database.query.apply(this, createQueries(type, rowData, columns, 'cached'));
          return database.query.apply(this, createQueries('getCached', rowData, columns));
        }
      },
      'updates': function (sinceTime) {
        return new Promise(function(fulfill, reject) {
        var rowData = {};
        rowData[lastUpdatedField] = sinceTime;
         Promise.all([
           actions.get.selectAll(rowData, 'selectSince'),
           actions.selectAllKeys(??)
        });
        somehow compare those keys with what has been written to 
      },
      'updatesSinceSync': function (sourceName) {
        return masterCache.selectLastUpdate({
          'source': sourceName
        }).then(function (updateTime) {
          return actions.get.updates(updateTime);
        });
      }
    },
    'save': function () {
      // Writes the changes to the original file
      Promise.all([
        database.query.apply(this, createQueries('getUpdated', undefined, columns)),
        database.query.apply(this, createQueries('getRemoved', undefined, columns))
      ]).then(function (results) {
        return writeToSource(results[0], results[1]);
      });
    },
    'close': function () {
      // Removes the database from memory
      return database.close();
    },
    'modify': {
      // The row object needs to contain column names and values
      // ex. {'column1': 'value', 'primaryKey': 1}
      'create': function (row) {
        // CHECK IF WE HAVE THIS VALUE IN ALL TABLES
        // IF NOT, INSERT IT TO THE UPDATE TABLE
        return actions.cache.selectAll(getPrimaryKeysOnly(primaryKeys, row))
          .then(function (results) {
            // console.log('conflict?', results, getPrimaryKeysOnly(primaryKeys, row));
            if (results.length > 0) {
              return promiseError('Cannot create, fields already exist.\n\t create: ' + JSON.stringify(row) + '\n\t existing: ' + JSON.stringify(results));
            } else {
              return updateRow(row);
            }
          });
      },
      'update': function (row) {
        // Basically an upsert
        return updateRow(row);
      },
      'remove': function (row) {
        // requires all primary keys
        return updateRow(row, true);
      },
      'applyUpdates': function(updates) {
      }
    }
  };

  return actions;
};

module.exports = function (sourceConfig, masterCache) {
  var sourceType = sourceConfig.connection && sourceConfig.connection.type;
  var sources = tools.requireDirectory(__dirname, [__filename], sourceType === 'json' ? ['json.js'] : undefined);
  var source = sources[sourceType];

  // TODO: Compare source permissions
  if (!source) {
    return promiseError('Invalid Source type specified in connection: ' + (sourceConfig.connection && sourceConfig.connection.type));
  } else if (!sourceConfig.name) {
    return promiseError('All sources must have a name\n\t' + JSON.stringify(sourceConfig, null, 2));
  } else {
    return new Promise(function (fulfill, reject) {
      var taskList = [{
        'name': 'dataToJson',
        'description': 'Converts the source into a JSON format',
        'task': sources[sourceConfig.connection.type],
        'params': [sourceConfig]
      }, {
        'name ': 'Create Database',
        'description': 'Creates a database from the JSON representation of the data and the columns',
        'task': createDatabase,
        'params': ['{{dataToJson.data}}', '{{dataToJson.columns}}', sourceConfig]
      }];

      tools.iterateTasks(taskList, 'create source').then(function (r) {
        var database = tools.arrayGetLast(r);
        var columns = r[0].columns; // TODO, should we use the columns from the db (r[1]) instead?
        var writeToSource = r[0].writeFn;
        var querySource = r[0].querySource;
        fulfill(new SourceObject(database, columns, writeToSource, querySource, masterCache));
      }).catch(reject);
    });
  }
};
