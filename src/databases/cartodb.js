var Promise = require('bluebird');
var superagent = require('superagent');
var fandlebars = require('fandlebars');

var format = function (output) {
  // TODO: Clean up the output from this so it's similar to all other outputs
  var result = [];
  if (output && output.text && output.text.rows) {
    result = output.text.rows;
    result.fields = output.text.fields;
  }
  return result;
};

module.exports = function (connectionConfig) {
  var returnObject = {
    query: function (query, params, returnRaw) {
      return new Promise(function (fulfill, reject) {
        var cleanedSql = fandlebars(query, params);
        var requestPath = 'https://' + connectionConfig.account + '.cartodb.com/api/v2/sql';

        if (cleanedSql.length > 5) {
          superagent.post(requestPath)
            .set('Content-Type', 'application/json')
            .set('Accept', 'application/json')
            .send({
              'q': cleanedSql,
              'api_key': connectionConfig.apiKey
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
