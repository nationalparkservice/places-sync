// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
var sources = tools.requireDirectory(__dirname, [__filename]);
var createDatabase = require('./helpers/jsonToSqlite');

var SourceObject = function (database, columns, sourceConfig) {
  // Query based on the primary key(s), or all keys if no primary keys exist
  var queryKey = tools.Arrayify(sourceConfig.primaryKey || tools.simplifyArray(columns));

  // Adds quotes around objects in an array
  // TODO: make this better
  var quoteArray = function (a) {
    // Makes it easy to query all columns
    return a.map(function (v) {
      return '"' + v + '"';
    });
  };

  var modify = {
    'insert': function (tableName, values) {},
    'remove': function (tableName, values) {}
  };

  var createWhereObj = function (keys, values) {
    // The keys must come in as an array
    // is no keys are passed in, it is assumed that all the columns are keys
    keys = keys || queryKey;
    if (!Array.isArray(keys)) {
      throw new Error('createWhereObj keys must be a string or an array.');
    }

    // Allow either an array or an object to be passed in for values
    var valuesObj = values;
    if (Array.isArray(values)) {
      valuesObj = {};
      keys.forEach(function (pk, i) {
        valuesObj[pk] = values[i];
      });
    }

    // If nothing is specified for a value, we just assume not null
    var defaultWhere = {
      '$ne': null
    };

    var whereObj = {};

    keys.forEach(function (pk) {
      whereObj[pk] = valuesObj[pk] || defaultWhere;
    });
    return whereObj;
  };

  var quotedColumns = function (tableName) {
    return quoteArray(tools.simplifyArray(columns)).map(function (c) {
      return '"' + tableName + '".' + c;
    }).join(',');
  };
  var quotedPrimaryKeys = quoteArray(tools.arrayify(sourceConfig.primaryKey || tools.simplifyArray(columns)));

  var selectAllQuery = 'SELECT ' + quotedColumns('all_data') + ' FROM (';
  selectAllQuery += ' SELECT ' + quotedColumns('source');
  selectAllQuery += ' FROM "source"';
  selectAllQuery += ' LEFT JOIN "remove" ON ' + quotedPrimaryKeys.map(function (pk) {
    return '"remove".' + pk + ' = "source".' + pk;
  }).join(' AND ');
  selectAllQuery += ' LEFT JOIN  "new" ON ' + quotedPrimaryKeys.map(function (pk) {
    return '"new".' + pk + ' = "source".' + pk;
  }).join(' AND ');
  selectAllQuery += ' WHERE';
  selectAllQuery += quotedPrimaryKeys.map(function (pk) {
    return '"remove".' + pk + ' IS NULL';
  }).join(' AND ');
  selectAllQuery += ' AND ';
  selectAllQuery += quotedPrimaryKeys.map(function (pk) {
    return '"new".' + pk + ' IS NULL';
  }).join(' AND ');
  selectAllQuery += ' UNION';
  selectAllQuery += ' SELECT ' + quotedColumns('new');
  selectAllQuery += ' FROM "new") AS "all_data"';

  var actions = {
    'selectAll': function (whereObj) {
      // Selects all fields, if not whereObj is supplied, it will query everything
      whereObj = whereObj || createWhereObj(queryKey);
      var where = tools.createWhereClause(whereObj);
      var query = selectAllQuery + ' WHERE ' + where[0] + ';';
      var params = where[1];
      return database.query(query, params);
    },
    'selectOne': function (queryKeyValues) {
      return new Promise(function (fulfill, reject) {
        if (!queryKeyValues) {
          reject(new Error('selectOne requires the values of the primary keys in an array'));
        } else if (!sourceConfig.primaryKey) {
          reject(new Error('Not primary keys are defined'));
        }
        queryKeyValues = tools.arrayify(queryKeyValues);
        var whereObj = tools.createWhereObj(queryKey, queryKeyValues);
        actions.selectAll(whereObj).then(function (r) {
          return r[0];
        }).catch(reject);
      });
    },
    'selectLastUpdateTime': function (whereObj) {
      // gets the max date from the lastUpdate field, a whereObj can be applied to this
    },
    'describe': function () {
      // Returns the columns and their data types
      // as well as what the primaryKey(s) is/are and the lastEdit field
      return JSON.parse(JSON.stringyify(columns));
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
      'create': function (rows) {
        // Checks for primarykey violations, and if none, then it will insert
        // CHECK IF WE HAVE THIS VALUE IN ALL TABLES
        // IF NOT, INSERT IT TO THE UPDATE TABLE
      },
      'update': function (rows) {
        // Basically an upsert
        // REMOVE this record from the REMOVE table
        // INSERT IT TO THE UPDATE TABLE
      },
      'remove': function (rows) {
        // requires all primary keys
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
        'name': 'Create Database',
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
