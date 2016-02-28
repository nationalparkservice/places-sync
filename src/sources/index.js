// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
var sources = tools.requireDirectory(__dirname, [__filename]);
var createDatabase = require('./helpers/jsonToSqlite');
var CreateQueries = require('./helpers/createQueries');

var verifyKeys = function (row, keys) {
  return new Promise(function (fulfill, reject) {
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
  var newObj;
  for (key in baseObject) {
    newObj[key] = newValues[key] !== undefined ? newValues[key] : baseObject[key];
  }
  return newObj;
};

var SourceObject = function (database, columns, sourceConfig) {
  // Query based on the primary key(s), or all keys if no primary keys exist
  var createQueries = new CreateQueries(columns, sourceConfig.primaryKey, sourceConfig.lastUpdate);
  var primaryKeys = tools.arrayify(sourceConfig.primaryKey);

  var actions = {
    'selectAll': function (whereObj) {
      // Selects all fields, if not whereObj is supplied, it will query everything
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
    },
    'saveAs': function (source) {
      // Writes the data to a new source
    },
    'close': function () {
      // Removes the database from memory
    },
    'modify': {
      // The row object needs to contain column names and values
      // ex. {'column1': 'value', 'primaryKey': 1}
      'create': function (row) {
        // CHECK IF WE HAVE THIS VALUE IN ALL TABLES
        // IF NOT, INSERT IT TO THE UPDATE TABLE
        return actions.selectAll(row)
          .then(function (results) {
            if (results.length > 0) {
              throw new Error('Cannot create, fields already exist');
            } else {
              // YAY?
              // I don't know if the promises will work like this, i probably need to wrap it in a promise at least
            }
          });
      },
      'update': function (row) {
        // Basically an upsert
        //
        // Checks for primarykey violations, and if none, then it will insert
        return verifyKeys(row, primaryKeys)
          .then(function () {
            return actions.selectAll(row);
          })
          .then(function (results) {
            // REMOVE this record from the REMOVE table
            // INSERT IT TO THE UPDATE TABLE
            return results.map(function (resultRow) {
              return createQueries('update', updateObject(resultRow, row), columns);
            }).all();
          });
      },
      'remove': function (row) {
        // requires all primary keys
        verifyKeys(row, primaryKeys);
      // CHECK IF WE HAVE THIS VALUE IN ALL TABLES
      // IF YES REMOVE this record from the UPDATE table
      // INSERT IT TO THE REMOVE TABLE
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

      tools.iterateTasks(taskList).then(function (r) {
        fulfill(new SourceObject(tools.arrayGetLast(r), r[0].columns, sourceConfig));
      }).catch(function (e) {
        reject(tools.arrayGetLast(e));
      });
    });
  }
};
