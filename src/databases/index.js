// A very basic way to run queries in various database formats
// Connections are mostly defined by the database types
// connection.type IS required by this step

var tools = require('../tools');
var Promise = require('bluebird');
var databases = tools.requireDirectory(__dirname, [__filename]);

var DatabaseObject = function (database) {
  var returnObject = {
    'query': function () {
      // console.log('q', query, params);
      return database.query.apply(this, arguments);
    },
    'queryList': function (query, paramList) {
      return Promise.all(paramList.map(function (params) {
        return returnObject.query(query, params);
      }));
    },
    'close': function () {
      return database.close();
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
