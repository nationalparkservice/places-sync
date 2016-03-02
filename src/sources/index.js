// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
var sources = tools.requireDirectory(__dirname, [__filename]);
var createDatabase = require('./helpers/jsonToSqlite');
var CreateQueries = require('./helpers/createQueries');

var promiseError = function (msg) {
  return new Promise(function (f, r) {
    r(new Error(msg));
  });
};

var verifyKeys = function (row, keys) {
  return new Promise(function (fulfill, reject) {
    console.log('verify', row, keys);
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

var updateObject = function (baseObject, newValues) {
  var key;
  var newObj = {};
  for (key in baseObject) {
    newObj[key] = newValues[key] !== undefined ? newValues[key] : baseObject[key];
  }
  return newObj;
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

var SourceObject = function (database, columns, writeToSource, sourceConfig) {
  // Query based on the primary key(s), or all keys if no primary keys exist
  var createQueries = new CreateQueries(columns, sourceConfig.primaryKey, sourceConfig.lastUpdate);
  var primaryKeys = tools.arrayify(sourceConfig.primaryKey);
  console.log(primaryKeys, sourceConfig.primaryKey, tools.arrayify(sourceConfig.primaryKey), sourceConfig);

  var updateRow = function (row, remove) {
    var updatedRowData;
    return verifyKeys(row, primaryKeys)
      .then(function () {
        return actions.selectAll(getPrimaryKeysOnly(primaryKeys, row));
      })
      .then(function (results) {
        return Promise.all(results.map(function (resultRow) {
          updatedRowData = updateObject(resultRow, row);
          // Remove any entries for this row in the removes table
          console.log('cleanRemove', createQueries('cleanRemove', resultRow, columns));
          return database.query.apply(this, createQueries('cleanRemove', resultRow, columns))
            .then(function () {
              // Remove any entries for this row in the Updates table
              console.log('cleanUpdate', createQueries('cleanUpdate', resultRow, columns));
              return database.query.apply(this, createQueries('cleanUpdate', resultRow, columns));
            }).then(function () {
              // Just for debug
              return database.query.apply(this, createQueries('_debug.allRemove', undefined, columns));
            });
        })).then(function (res) {
          // Insert this into the update or remove table
          console.log('res?', res, remove);
          var query = createQueries('run' + (remove ? 'Remove' : 'Update'))[0];
          var completeRow = updatedRowData || getDefaults(row, columns);
          console.log('insert' + (remove ? 'Remove' : 'Update'), query, completeRow);
          return database.query(query, completeRow);
        });
      });
  };

  var actions = {
    'selectAll': function (whereObj) {
      // Selects all fields, if not whereObj is supplied, it will query everything
      console.log(createQueries('selectAll', whereObj, columns));
      return database.query.apply(this, createQueries('selectAll', whereObj, columns));
    },
    'selectLastUpdate': function (whereObj) {
      // gets the max date from the lastUpdate field, a whereObj can be applied to this
      return new Promise(function (fulfill, reject) {
        database.query.apply(this, createQueries('selectLastUpdate', whereObj)).then(function (r) {
          fulfill(r[0].lastUpdate);
        }).catch(reject);
      });
    },
    'describe': function () {
      // Returns the columns and their data types
      // as well as what the primaryKey(s) is/are and the lastEdit field
      return JSON.parse(JSON.stringify(columns));
    },
    'save': function () {
      // Writes the changes to the original file
      return writeToSource(updated, removed);
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
        return actions.selectAll(getPrimaryKeysOnly(primaryKeys, row))
          .then(function (results) {
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
      }
    }
  };

  return actions;
};

module.exports = function (sourceConfig, lastUpdate) {
  var source = sources[sourceConfig.connection && sourceConfig.connection.type];
  if (!source) {
    throw new Error('Invalid Source type specified in connection: ' + sourceConfig.connection && sourceConfig.connection.type);
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
        var columns = r[0].columns;
        var writeToSource = r[0].writeFn;
        fulfill(new SourceObject(database, columns, writeToSource, sourceConfig));
      }).catch(reject);
    });
  }
};
