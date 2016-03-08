// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
// var sources = tools.requireDirectory(__dirname, [__filename]);
var createDatabase = require('./helpers/jsonToSqlite');
var CreateQueries = require('./helpers/createQueries');

var dummyPromise = function (fulfillMsg, rejectMsg) {
  return new Promise(function (fulfill, reject) {
    if (rejectMsg) {
      reject(rejectMsg);
    } else {
      fulfill(fulfillMsg);
    }
  });
};

var rowsToMaster = function (rows, columns, primaryKeys, lastUpdatedField, removedField, removeRow, sourceName) {
  return rows.map(function (row) {
    var key = primaryKeys.map(function (k) {
      return row[k];
    }).join(',');
    var hash = tools.md5(columns.map(function (c) {
      return row[c.name];
    }).join(','));
    return {
      'key': key,
      'process': 'sync',
      'source': sourceName,
      'hash': hash,
      'last_updated': row[lastUpdatedField],
      'is_removed': removeRow ? 1 : 0
    };
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
  var row = row || {};
  primaryKeys.forEach(function (pk) {
    rowPrimaryKeys[pk] = row[pk];
  });
  return rowPrimaryKeys;
};

var SourceObject = function (database, columns, writeToSource, querySource, masterCache, sourceConfig) {
  // Query based on the primary key(s), or all keys if no primary keys exist
  var primaryKeys = tools.simplifyArray(columns.filter(function (c) {
    return c.primaryKey;
  }));
  var lastUpdatedField = tools.simplifyArray(columns.filter(function (c) {
    return c.lastUpdatedField;
  }))[0];
  var removedField = tools.simplifyArray(columns.filter(function (c) {
    return c.removedField;
  }))[0];

  var createQueries = new CreateQueries(columns, primaryKeys, lastUpdatedField);

  var updateRow = function (row, remove) {
    var updatedRowData = [];

    // Make sure all the required keys are present in the row
    return verifyKeys(row, primaryKeys)
      .then(function () {
        // Check the cache (and upload / remove table) for any of these rows
        // If any do exist, remove them, since we are removing them anyway
        return actions.cache.selectAll(getPrimaryKeysOnly(primaryKeys, row));
      })
      .then(function (results) {
        return Promise.all(results.map(function (resultRow) {
          updatedRowData.push(tools.updateObject(resultRow, row));

          // Remove any entries for this row in the removes table
          return database.query.apply(this, createQueries('cleanRemove', resultRow, columns))
            .then(function () {
              // Remove any entries for this row in the Updates table
              return database.query.apply(this, createQueries('cleanUpdate', resultRow, columns));
            });
        })).then(function (res) {
          // Insert this into the update or remove table
          var query = createQueries('run' + (remove ? 'Remove' : 'Update'))[0];

          // If any columns are missing, fill them in with default values
          var completeRow = getDefaults(row, columns);

          // Run the query on the database
          return database.query(query, completeRow);
        });
      });
  };

  var actions = {
    'cache': {
      'selectAll': function (rowData, requestedColumns) {
        requestedColumns = requestedColumns || columns;
        // Selects all fields, if not rowData is supplied, it will query everything
        return database.query.apply(this, createQueries('selectAllInCache', rowData, requestedColumns));
      }
    },
    'get': {
      'columns': function () {
        // Returns the columns and their data types
        // as well as what the primaryKey(s) is/are and the lastEdit field
        return JSON.parse(JSON.stringify(columns));
      },
      'all': function (rowData, type) {
        // Returns all data from the source matching the rowData
        // this is strange in that it allows "selectSince" to be passed in for type
        // doing this will change the query to query information greater than the lastUpdatedField in the rowData
        type = type !== 'selectSince' ? 'select' : type; // Prevent anything weird from getting passed in
        if (querySource) {
          return querySource(type, rowData, columns);
        } else {
          return database.query.apply(this, createQueries(type, rowData, columns, 'cached'));
        }
      },
      'allKeys': function (rowData) {
        // Gets all keys in source table
        if (querySource) {
          // We can query the source directly
          return querySource('select', rowData, primaryKeys);
        } else {
          // We have to query the cache
          return actions.cache.selectAll(getPrimaryKeysOnly(primaryKeys, rowData), primaryKeys);
        }
      },
      'lastUpdate': function (rowData) {
        // gets the max date from the lastUpdate field, a rowData can be applied to this
        // Should tell you to last time the source was updated
        // if rowData is applied, it will use that as a where clause / filter
        return new Promise(function (fulfill, reject) {
          if (lastUpdatedField) {
            if (querySource) {
              querySource('selectLastUpdate', rowData).then(function (r) {
                fulfill(r[0].lastUpdate);
              }).catch(reject);
            } else {
              database.query.apply(this, createQueries('selectLastUpdate', rowData, undefined, 'cached')).then(function (r) {
                fulfill(r[0].lastUpdate);
              }).catch(reject);
            }
          } else {
            fulfill(-1); // Return -1 is there is no "lastUpdatedField"
          }
        });
      },
      'updates': function (otherSourceName) {
        return new Promise(function (fulfill, reject) {
          var masterCacheQuery = {
            'process': 'sync',
            'source': otherSourceName
          };
          var tasks = [{
            'name': 'lastSyncTime',
            'description': 'Gets the last time this source was updated from the master cache',
            'task': masterCache ? masterCache.get.lastUpdate : dummyPromise,
            'params': [masterCache ? masterCacheQuery : -1] // Return -1 if there is no master cache
          }, {
            'name': 'syncTimeQueryObj',
            'description': 'Create the query object for the database',
            'example': 'in: (lastUpdatedField , 5) -- out: {"lastUpdatedField": 5}',
            'task': tools.setProperty,
            'params': [lastUpdatedField, '{{lastSyncTime}}']
          }, {
            'name': 'updatedSinceTime',
            'description': 'Get everything that has been updated since a certain time',
            'task': actions.get.all,
            'params': ['{{syncTimeQueryObj}}', 'selectSince']
          }, {
            'name': 'allKeys',
            'description': 'Get ALL keys from this source, so we can tell what was deleted',
            'task': actions.get.allKeys,
            'params': []
          }, {
            'name': 'masterCacheKeys',
            'description': 'Get ALL keys from the master cache, so we can tell what used to exist that doesnt anymore',
            'task': masterCache ? masterCache.get.allKeys : dummyPromise,
            'params': [masterCache ? masterCacheQuery : []]
          }];
          tools.iterateTasks(tasks).then(function (results) {
            var exp = {};
            for (var k in results) {
              if (parseInt(k, 10).toString() !== k.toString()) {
                exp[k] = results[k];
              }
            }
            console.log(JSON.stringify(exp, null, 2));
            var updated = results.updatedSinceTime;
            // var existingKeys = [];
            var existingMasterCacheIndexes = [];
            var removed = [];

            var masterCacheKeysOnly = results.masterCacheKeys.map(function (row) {
              return row.key;
            });
            results.allKeys.forEach(function (currentRow) {
              var temp = [];
              primaryKeys.forEach(function (k) {
                temp.push(currentRow[k]);
              });
              var masterCacheKeysIndex = masterCacheKeysOnly.indexOf(temp.join(','));
              if (masterCacheKeysIndex > -1) {
                // existingKeys.push(currentRow);
                existingMasterCacheIndexes.push(masterCacheKeysIndex);
              }
            });
            removed = masterCacheKeysOnly.filter(function (key, index) {
              return existingMasterCacheIndexes.indexOf(index) === -1;
            }).map(function (value) {
              var cols = value.split(',');
              var returnObj = {};
              primaryKeys.forEach(function (pk, idx) {
                returnObj[pk] = cols[idx];
              });
              return returnObj;
            });
            fulfill({
              'updated': updated,
              'removed': removed
            });
          }).catch(reject);
        });
      }
    },
    'save': function () {
      // Writes the changes to the original file
      return Promise.all([
        database.query.apply(this, createQueries('getUpdated', undefined, columns)),
        database.query.apply(this, createQueries('getRemoved', undefined, columns))
      ]).then(function (results) {
        return Promise.all([
          masterCache ? masterCache.modify.applyUpdates({
            updated: rowsToMaster(results[0], columns, primaryKeys, lastUpdatedField, removedField, false, sourceConfig.name),
            removed: rowsToMaster(results[1], columns, primaryKeys, lastUpdatedField, removedField, true, sourceConfig.name)
          }) : dummyPromise(),
          writeToSource(results[0], results[1]),
          masterCache ? masterCache.save() : dummyPromise()
        ]);
      });
    },
    'close': function () {
      // Removes the database from memory
      return database.close();
    },
    'modify': {
      // The row object needs to contain column names and values
      // ex. {'column1': 'value', 'column2': 2}
      'create': function (row) {
        // CHECK IF WE HAVE THIS VALUE IN ALL TABLES
        // IF NOT, INSERT IT INTO THE UPDATED TABLE
        return actions.cache.selectAll(getPrimaryKeysOnly(primaryKeys, row))
          .then(function (results) {
            // console.log('conflict?', results, getPrimaryKeysOnly(primaryKeys, row));
            if (results.length > 0) {
              return dummyPromise(undefined, 'Cannot create, fields already exist.\n\t create: ' + JSON.stringify(row) + '\n\t existing: ' + JSON.stringify(results));
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
      'applyUpdates': function (updates) {
        // takes an object {'updated': [rows], 'removed': [rows]}
        // Then applies it to the updated and removed tables
        // Probably just options up the object and loops the updates and removes
        var tasks = [];
        updates.removed.forEach(function (row) {
          tasks.push(actions.modify.remove(row));
        });
        updates.updated.forEach(function (row) {
          tasks.push(actions.modify.update(row));
        });
        return Promise.all(tasks);
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
    return dummyPromise(undefined, 'Invalid Source type specified in connection: ' + (sourceConfig.connection && sourceConfig.connection.type));
  } else if (!sourceConfig.name) {
    return dummyPromise(undefined, 'All sources must have a name\n\t' + JSON.stringify(sourceConfig, null, 2));
  } else {
    return new Promise(function (fulfill, reject) {
      var taskList = [{
        'name': 'dataToJson',
        'description': 'Converts the source into a JSON format',
        'task': sources[sourceConfig.connection.type],
        'params': [sourceConfig]
      }, {
        'name': 'Create Database',
        'description': 'Creates a database from the JSON representation of the data and the columns',
        'task': createDatabase,
        'params': ['{{dataToJson.data}}', '{{dataToJson.columns}}', sourceConfig]
      }];

      tools.iterateTasks(taskList, 'create source').then(function (r) {
        var database = tools.arrayGetLast(r);
        var columns = r[0].columns; // TODO, should we use the columns from the db (r[1]) instead?
        var writeToSource = r[0].writeFn;
        var querySource = r[0].querySource;
        fulfill(new SourceObject(database, columns, writeToSource, querySource, masterCache, sourceConfig));
      }).catch(reject);
    });
  }
};
