var Promise = require('bluebird');
var pg = require('pg');
var fandlebars = require('fandlebars');

var format = function (output) {
  // TODO: Clean up the output from this so it's similar to all other outputs
  return output.rows;
};

var readParams = function (rawQuery, objParams) {
  // Creates a parameterized query for postgres
  var re = function (name) {
    return new RegExp('{{' + name + '}}', 'g');
  };
  var returnValue = {
    query: rawQuery,
    params: [],
    tempParams: {}
  };
  if (objParams && Object.prototype.toString.call(objParams) === '[object Object]') {
    for (var paramIndex in objParams) {
      if (rawQuery.match(re(paramIndex))) {
        if (objParams[paramIndex] === null) {
          // Postgresql doesn't support null as a parameter
          returnValue.tempParams[paramIndex] = 'null';
        } else {
          returnValue.tempParams[paramIndex] = '$' + (returnValue.params.push(objParams[paramIndex]));
        }
      }
    }

    returnValue.query = fandlebars(rawQuery, returnValue.tempParams);
    delete returnValue.tempParams;
  }
  return returnValue;
};

module.exports = function (sourceConfig) {
  var connectionConfig = sourceConfig.connection;
  var connectionString = fandlebars('postgres://{{user}}:{{password}}@{{host}}' + (connectionConfig.port ? ':{{port}}' : '') + '/{{database}}', connectionConfig);
  var returnObject = {
    query: function (query, params, returnRaw) {
      return new Promise(function (fulfill, reject) {
        // Runs an individual SQL query
        var newParams = null;
        var newStack = new Error();
        var error;
        if (params && Object.prototype.toString.call(params) === '[object Object]') {
          newParams = readParams(query, params);
          query = newParams.query;
          params = newParams.params;
        }

        pg.connect(connectionString, function (err, client, done) {
          if (err) {
            done();
            pg.end();
            error = new Error(err.message + '\n\terrno: ' + err.errno + '\n\tcode: ' + err.code + '\n\tQuery: ' + query + '\n\tParams: ' + params.toString() + '\n');
            error.stack = err.stack + newStack.stack.replace(/^Error\n/, '\n');
            error.errno = err.errno;
            error.code = err.code;
            reject(error);
          } else {
            client.query(query, params, function (e, result) {
              done();
              pg.end();
              if (e) {
                error = new Error(e.message + '\n\terrno: ' + e.errno + '\n\tcode: ' + e.code + '\n\tQuery: ' + query + '\n\tParams: ' + JSON.stringify(params) + '\n');
                error.stack = e.stack + newStack.stack.replace(/^Error\n/, '\n');
                error.errno = e.errno;
                error.code = e.code;
                reject(error);
              } else {
                fulfill(returnRaw ? result : format(result));
              }
            });
          }
        });
      });
    },
    close: function () {
      return new Promise(function (fulfill, reject) {
        // Disconnects all idle clients within all active pools, and has all
        // client pools terminate. Any currently open, checked out clients will
        // still need to be returned to the pool before they will be shut down
        // and disconnected.

        fulfill(pg.end());
      });
    }
  };
  return returnObject;
};
