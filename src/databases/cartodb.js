var Promise = require('bluebird');
var superagent = require('superagent');
var fandlebars = require('fandlebars');
var tools = require('../tools');

var format = function (output) {
  // TODO: Clean up the output from this so it's similar to all other outputs
  var result = [];
  if (output && output.body && output.body.rows) {
    result = output.body.rows;
    result.fields = output.body.fields;
  }
  return result;
};

var parameterize = function (param, type) {
  // CartoDB doesn't really support parameterized queries, so we'll do them our own way
  param = tools.normalizeToType(param);
  if (param === null || param === undefined) {
    return 'null';
  } else {
    param = param.toString();
  }
  var returnValue = "convert_from(decode('";
  type = type || tools.getDataType(param);
  if (type !== 'text') {
    returnValue = "'" + param.replace(/\'/g, "''") + "'::" + type;
  } else {
    for (var i = 0; i < param.length; i++) {
      returnValue += param.charCodeAt(i).toString(16);
    }
    returnValue += "', 'hex'), 'UTF-8')::" + type;
  }

  return returnValue;
};
var parameterizeQuery = function (query, params, columns) {
  params = JSON.parse(JSON.stringify(params || {}));
  for (var column in params) {
    var matchedColumn = columns && columns.filter(function (c) {
      return c.name === column;
    })[0];
    params[column] = parameterize(params[column], matchedColumn && !matchedColumn.transformed && matchedColumn.type);
  }
  return fandlebars(query, params);
};

module.exports = function (connectionConfig) {
  var returnObject = {
    query: function (query, params, returnRaw, columns) {
      return new Promise(function (fulfill, reject) {
        // var cleanedSql = fandlebars(query, params);
        var cleanedSql = parameterizeQuery(query, params, columns);
        var requestPath = 'https://' + connectionConfig.connection.account + '.cartodb.com/api/v2/sql';
        
        if (cleanedSql.length > 5) {
          superagent.post(requestPath)
            .set('Content-Type', 'application/json')
            .set('Accept', 'application/json')
            .send({
              'q': cleanedSql,
              'api_key': connectionConfig.connection.apiKey
            })
            .end(function (err, response) {
              if (err || response.error) {
                reject(new Error(JSON.stringify(err || response, null, 2)));
              } else {
                fulfill(returnRaw ? response : format(response));
              }
            });
        } else {
          reject('Query Too Short: (' + cleanedSql.length + ') chars');
        }
      });
    },
    close: function () {
      return new Promise(function (fulfill, reject) {
        // Dummy function, cartodb connections close as soon as the query is done
        fulfill(true);
      });
    }
  };
  return returnObject;
};
