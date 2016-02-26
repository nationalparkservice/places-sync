var tools = require('./tools');
var Promise = require('bluebird');

// TODO, load the datarz
var loadSourceData = function () {
  return arguments;
};

var createSqliteDb = function () {
  return arguments;
};

var Source = function () {
  return {
    'selectAll': function (whereObj) {
      // Selects all fields, if not whereObj is supplied, it will query everything
    },
    'selectOne': function (primaryKey) {},
    'selectLastUpdateTime': function (whereObj) {
      // gets the max date from the lastUpdate field, a whereObj can be applied to this
    },
    'describe': function () {
      // Returns the columns and their data types
      // as well as what the primaryKey(s) is/are and the lastEdit field
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

module.exports = function (cache) {
  return function (sourceConfig) {
    return new Promise(function (fulfill, reject) {
      var sourceGuid = tools.guid();
      var tasks = [{
        'name': 'getLastRead',
        'description': 'Determines when this specific source was last read so that we only need to read new records',
        'task': cache ? cache.selectLastUpdateTime : function () {
          return 0;
        },
        'params': [{
          'source': sourceConfig.name
        }]
      }, {
        'name': 'loadData',
        'descripion': 'Loads data into the memory object, it should only load new data in',
        'task': loadSourceData,
        'params': [sourceConfig, '{{sourceDatabase}}', '{{getLastRead}}']
      }];
      tools.iterateTasks(tasks).then(function (r) {
        fulfill(new Source(sourceConfig, r));
      }).cache(function (e) {
        reject(tools.arrayGetLast(e));
      });
    });
  };
};
