// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
var sources = tools.requireDirectory(__dirname, [__filename]);
var createDatabase = require('./helpers/jsonToSqlite');

var CreateQueries = function(columns, primaryKey, lastUpdate) {
  var queryKey = tools.arrayify(primaryKey || tools.simplifyArray(columns));

  var arrayToColumns = function(columns, tableName) {
    return tools.simplifyArray(columns).map(function(c) {
      return (tableName ? '"' + tableName + '".' : '') + '"' + c + '"';
    }).join(',');
  };

  var arraysToObj = function(keys, values) {
    // Takes two arrays ['a','b','c'], [1,2,3]
    // And makes an object {'a':1,'b':2,'c':3}
    var returnObject = {};
    if (Array.isArray(values)) {
      keys.forEach(function(key, i) {
        returnObject[key] = values[i];
      });
    } else if (typeof values === 'object') {
      returnObject = values;
    }
    return returnObject;
  };

  var createWhereObj = function(keys, values, defaultWhere) {
    var valuesObj = arraysToObj(keys, values);
    var whereObj = {};

    // If nothing is specified for a value, the default is (null or not null)
    defaultWhere = defaultWhere || {
      '$or': [{
        '$eq': null
      }, {
        '$ne': null
      }]
    };

    // Add the default value where nothing else is
    keys.forEach(function(pk) {
      whereObj[pk] = valuesObj[pk] || defaultWhere;
    });

    return tools.createWhereClause(whereObj);
  };

  var queries = {
    'selectAll': function() {
      var selectAllQuery = 'SELECT ' + arrayToColumns(columns, 'all_data') + ' FROM (';
      selectAllQuery += ' SELECT ' + arrayToColumns(columns, 'source');
      selectAllQuery += ' FROM "source"';
      selectAllQuery += ' LEFT JOIN "remove" ON ' + queryKey.map(function(pk) {
        return '"remove"."' + pk + '" = "source"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' LEFT JOIN  "new" ON ' + queryKey.map(function(pk) {
        return '"new".' + pk + ' = "source"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' WHERE';
      selectAllQuery += queryKey.map(function(pk) {
        return '"remove"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' AND ';
      selectAllQuery += queryKey.map(function(pk) {
        return '"new"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' UNION';
      selectAllQuery += ' SELECT ' + arrayToColumns(columns, 'new');
      selectAllQuery += ' FROM "new") AS "all_data"';
      return selectAllQuery;
    },
    'selectLastUpdate': function() {
      if (lastUpdate) {
        return 'SELECT MAX("all_data"."' + lastUpdate + '" AS "lastUpdate") FROM ' + queries.selectAllQuery + ') AS "last_update"';
      } else {
        return 'SELECT 0 AS "lastUpdate" ';
      }
    }
  };
  return function(queryName, values, keys) {
    var where = createWhereObj(tools.simplifyArray(keys || queryKey), values);
    var query = queries[queryName]() + ' WHERE ' + where[0];
    return [query, where[1]];
  };
};

var SourceObject = function(database, columns, sourceConfig) {
  // Query based on the primary key(s), or all keys if no primary keys exist
  var createQueries = new CreateQueries(columns, sourceConfig.primaryKey, sourceConfig.lastUpdate);

  var actions = {
    'selectAll': function(whereObj) {
      // Selects all fields, if not whereObj is supplied, it will query everything
      return database.query.apply(this, createQueries('selectAll', whereObj, columns));
    },
    'selectLastUpdate': function(whereObj) {
      // gets the max date from the lastUpdate field, a whereObj can be applied to this
      return new Promise(function(fulfill, reject) {
        database.query.apply(this, createQueries('selectLastUpdate', whereObj)).then(function(r) {
          fulfill(r[0].lastUpdate);
        }).catch(reject);
      });
    },
    'describe': function() {
      // Returns the columns and their data types
      // as well as what the primaryKey(s) is/are and the lastEdit field
      return JSON.parse(JSON.stringify(columns));
    },
    'save': function() {
      // Writes the changes to the original file
    },
    'saveAs': function(source) {
      // Writes the data to a new source
    },
    'close': function() {
      // Removes the database from memory
    },
    'modify': {
      'create': function(rows) {
        // Checks for primarykey violations, and if none, then it will insert
        // CHECK IF WE HAVE THIS VALUE IN ALL TABLES
        // IF NOT, INSERT IT TO THE UPDATE TABLE
      },
      'update': function(rows) {
        // Basically an upsert
        // REMOVE this record from the REMOVE table
        // INSERT IT TO THE UPDATE TABLE
      },
      'remove': function(rows) {
        // requires all primary keys
        // CHECK IF WE HAVE THIS VALUE IN ALL TABLES
        // IF YES REMOVE this record from the UPDATE table
        // INSERT IT TO THE REMOVE TABLE
      }
    }
  };

  return actions;
};

module.exports = function(sourceConfig, lastUpdate) {
  var source = sources[sourceConfig.connection && sourceConfig.connection.type];
  if (!source) {
    throw new Error('Invalid Source type specified in connection: ' + sourceConfig.connection && sourceConfig.connection.type);
  } else {
    return new Promise(function(fulfill, reject) {
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

      tools.iterateTasks(taskList).then(function(r) {
        fulfill(new SourceObject(tools.arrayGetLast(r), r[0].columns, sourceConfig));
      }).catch(function(e) {
        reject(tools.arrayGetLast(e));
      });
    });
  }
};
