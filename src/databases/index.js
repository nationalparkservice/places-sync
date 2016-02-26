// A very basic way to run queries in various database formats
// Connections are mostly defined by the database types
// connection.type IS required by this step

var Promise = require('bluebird');
var tools = require('../tools');
var databases = tools.requireDirectory(__dirname, [__filename]);

var DatabaseObject = function (database) {
  var returnObject = {
    'query': function (query, params) {
      return database.query(query, params);
    },
    'queryList': function (query, paramList) {
      return new Promise(function (fulfill, reject) {
        tools.iterateTasks(paramList.map(function (params, index, orig) {
          return {
            'name': 'Running Query List Item [' + (index + 1) + '/' + orig.length + ']',
            'task': returnObject.query,
            'params': [query, params]
          };
        })).then(fulfill).catch(function (e) {
          reject(tools.arrayGetLast(e));
        });
      });
    }
  };
  return returnObject;
};

module.exports = function (connectionConfig) {
  var database = databases[connectionConfig.type](connectionConfig);
  if (!database) {
    throw new Error('Invalid Database type specified in connection: ' + connectionConfig.type);
  } else {
    return new DatabaseObject(database);
  }
};
