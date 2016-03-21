var CreateQueries = require('../helpers/createQueries');
var ModifySource = require('./modifySource');
var Promise = require('bluebird');
var columnsToKeys = require('../helpers/columnsToKeys');
var extractPrimaryKeys = require('../helpers/extractPrimaryKeys');
var getUpdates; // Defined in the function
var rowsToMaster = require('../helpers/rowsToMaster');
var tools = require('../../tools');
var validateRow = require('../helpers/validateRow');

module.exports = function (database, columns, writeToSource, querySource, masterCache, sourceConfig) {
  // Load the basic query helpers
  var keys = columnsToKeys(columns);
  var createQueries = new CreateQueries(columns, keys.primaryKeys, keys.lastUpdatedField);
  var modifySource = new ModifySource(database, columns);

  // Define the actions
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
        type = type !== 'selectSince' ? 'select' : type; // Prevent anything other than the two acceptable values from getting passed in
        if (querySource) {
          return querySource(type, rowData, columns);
        } else {
          return database.query.apply(this, createQueries(type, rowData, columns, 'cached'));
        // since we pull from cached, we require a save before this will update
        // pulling from cache.selectAll will fix that
        // but we don't have a select since query for that (yet?)
        // return actions.cache.selectAll(
        }
      },
      'allKeys': function (rowData) {
        // We want to get all keys as well as the date field
        var queryKeys = JSON.parse(JSON.stringify(keys.primaryKeys));
        queryKeys.push(keys.lastUpdatedField);
        if (keys.hashField) {
          queryKeys.push(keys.hashField);
        }
        // Gets all keys in source table
        if (querySource) {
          // We can query the source directly
          return querySource('select', rowData, queryKeys);
        } else {
          // We have to query the cache
          return actions.cache.selectAll(extractPrimaryKeys(keys.primaryKeys, rowData), queryKeys);
        }
      },
      'lastUpdate': function (rowData) {
        // gets the max date from the lastUpdate field, a rowData can be applied to this
        // Should tell you to last time the source was updated
        // if rowData is applied, it will use that as a where clause / filter
        return new Promise(function (fulfill, reject) {
          if (keys.lastUpdatedField) {
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
        getUpdates = getUpdates || require('../helpers/getUpdates');
        return new Promise(function (fulfill, reject) {
          var masterCacheQuery = {
            'process': sourceConfig.connection.processName || 'sync',
            'source': otherSourceName
          };
          var orderedTasks = [{
            'name': 'lastSyncTime',
            'description': 'Gets the last time this source was updated from the master cache',
            'task': masterCache ? masterCache.get.lastUpdate : tools.dummyPromise,
            'params': [masterCache ? masterCacheQuery : -1] // Return -1 if there is no master cache
          }, {
            'name': 'syncTimeQueryObj',
            'description': 'Create the query object for the database',
            'example': 'in: (lastUpdatedField , 5) -- out: {"lastUpdatedField": 5}',
            'task': tools.setProperty,
            'params': [keys.lastUpdatedField, '{{lastSyncTime}}']
          }, {
            'name': 'updatedSinceTime',
            'description': 'Get everything that has been updated since a certain time',
            'task': actions.get.all,
            'params': ['{{syncTimeQueryObj}}', 'selectSince']
          }];
          var unorderedTasks = [{
            'name': 'orderedTasks',
            'task': tools.iterateTasks,
            'params': [orderedTasks]
          }, {
            'name': 'allKeys',
            'description': 'Get ALL keys from this source, so we can tell what was deleted',
            'task': actions.get.allKeys,
            'params': []
          }, {
            'name': 'masterCacheKeys',
            'description': 'Get ALL keys from the master cache, so we can tell what used to exist that doesnt anymore',
            'task': masterCache ? masterCache.get.allKeys : tools.dummyPromise,
            'params': [masterCache ? masterCacheQuery : []]
          }];
          Promise.all(
            unorderedTasks.map(function (task) {
              return task.task.apply(this, task.params);
            })
          ).then(function (promiseResults) {
            var results = promiseResults[0];
            results.allKeys = promiseResults[1];
            results.masterCacheKeys = promiseResults[2];
            getUpdates(results.lastSyncTime, results.updatedSinceTime, results.allKeys, results.masterCacheKeys, keys).then(function (values) {
              fulfill(values);
            }).catch(reject);
          }).catch(reject);
        });
      },
      '_database': function () {
        if (sourceConfig && sourceConfig.connection && sourceConfig.connection.returnDatabase) {
          return database;
        } else {
          throw new Error('Database is not supported with this connection');
        }
      }
    },
    'save': function () {
      // Writes the changes to the original file
      // TODO: Should this return the updates/removes that it saved?
      return Promise.all([
        database.query.apply(this, createQueries('getUpdated', undefined, columns)),
        database.query.apply(this, createQueries('getRemoved', undefined, columns))
      ]).then(function (results) {
        return Promise.all([
          masterCache ? masterCache.modify.applyUpdates({
            updated: rowsToMaster(results[0], columns, sourceConfig.name, sourceConfig.connection.processName, false),
            removed: rowsToMaster(results[1], columns, sourceConfig.name, sourceConfig.connection.processName, true)
          }) : tools.dummyPromise(),
          writeToSource(results[0], results[1]),
          modifySource.refresh(results[0], results[1])
        ]).then(function () {
          var fn = masterCache ? masterCache.save : tools.dummyPromise;
          return fn().then(function () {
            return tools.dummyPromise({
              'updated': results[0],
              'removed': results[1]
            });
          });
        });
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
        return actions.cache.selectAll(extractPrimaryKeys(keys.primaryKeys, row))
          .then(function (results) {
            if (results.length > 0) {
              return tools.dummyPromise(undefined, 'Cannot create, fields already exist.\n\t create: ' + JSON.stringify(row) + '\n\t existing: ' + JSON.stringify(results));
            } else {
              return modifySource.update(row);
            }
          });
      },
      'update': function (row) {
        // Basically an upsert
        return modifySource.update(row);
      },
      'remove': function (row) {
        // requires all primary keys
        return modifySource.update(row, true);
      },
      'applyUpdates': function (updates) {
        // takes an object with the following options
        //    'updated' - rows that have been updated
        //    'removed' - rows that no longer exist in the user table
        //    'created' - rows that previously didn't exist, but will be added to the updates table
        //    'existing' - rows that exist in both master and this data set (no action)
        //    'merged conflict' - rows in user table that don't match what's in the master
        //                        no action for these at this time, but may be useful for a
        //                        two way sync
        //    'conflict' - rows that have been added since the last sync that have also been added to
        //                 the user table but do not match our values
        //    'missing' - values that existed in the user that are not in the master table
        //    'created elsewhere' - missing values that were created since the last sync
        //    'unknown' - any other outcome. These will be monitored and explained
        // Then applies it to the updated and removed tables
        // Probably just options up the object and loops the updates and removes
        var tasks = [];
        var typesToRemove = ['removed'];
        typesToRemove.forEach(function (type) {
          tools.arrayify(updates[type]).forEach(function (row) {
            tasks.push(actions.modify.remove(row));
          });
        });
        var typesToInsert = ['updated', 'created', 'missing'];
        typesToInsert.forEach(function (type) {
          tools.arrayify(updates[type]).forEach(function (row) {
            // Make sure we have all the information (don't just push keys)
            if (validateRow(row, columns)) {
              tasks.push(actions.modify.update(row));
            }
          });
        });
        // TODO: Should this return the updates it applied?
        // TODO: Support the other actions for two-way sync
        return Promise.all(tasks);
      }
    }
  };
  return actions;
};
