// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
var sources = tools.requireDirectory(__dirname, [__filename]);
var createDatabase = require('./helpers/jsonToSqlite');

var SourceObject = function (database, columns, sourceConfig) {
  var quoteArray = function(a) {
    // Makes it easy to query all columns
    return a.map(function(v) {return '"' + v +'"';}).join(', ');
  };
  return {
    'selectAll': function (whereObj) {
      // Selects all fields, if not whereObj is supplied, it will query everything
      var query = 'SELECT ' + quoteArray(columns) + ' FROM source UGH'; // TODO WHERE OBJ
      var query = 'SELECT ' + quoteArray(columns) + ' FROM new'; // TODO WHERE OBJ
      var query = 'SELECT ' + quoteArray(columns) + ' FROM remove'; // TODO WHERE OBJ
    },
    'selectOne': function (primaryKey) {},
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
      },
      'update': function (rows) {
        // Basically an upsert
      },
      'remove': function (rows) {
        // requires all primary keys
      }
    }
  };
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
