var Promise = require('bluebird');
var sqlite = require('sqlite3');
var fandlebars = require('fandlebars');
// var tools = require('../tools');

var convertParams = function (origQuery, origParams) {
  origParams = origParams || {};
  var fn = Object.keys(origParams).length > 900 ? convertManyParams : convertFewParams;
  return fn(origQuery, origParams);
};

var convertFewParams = function (origQuery, origParams) {
  // Creates a parameterized query for sqlite3
  var re = function (name) {
    return new RegExp('{{' + name + '}}', 'g');
  };
  var r = {
    newQuery: origQuery.replace(re('.+?'), '?'),
    newParams: (origQuery.match(re('.+?')) || []).map(function (field) {
      return fandlebars(field, origParams, null, true)[field];
    })
  };
  return r;
};

var convertManyParams = function (origQuery, origParams) {
  var addParameter = function (param) {
    // These queries tend to have too many parameters for parameterized queries
    // http://www.sqlite.org/limits.html#max_variable_number (Defaults to 999)
    // Even in this fairly safe environment (loading from predefined files) it is
    // bad practice to load strings right into SQL. So our other option is to convert
    // all input to binary objects: http://www.sqlite.org/lang_expr.html
    // BLOB literals are string literals containing hexadecimal data and preceded by a single "x" or "X" character. Example: X'53514C697465'
    var returnValue = " CAST(x'";
    for (var i = 0; i < param.length; i++) {
      returnValue += param.charCodeAt(i).toString(16);
    }
    return returnValue + "' AS TEXT) ";
  // return '"' + param + '"';
  };
  var newParams = {};
  for (var param in origParams) {
    newParams[param] = addParameter(origParams[param]);
  }
  var r = {
    newQuery: fandlebars(origQuery, newParams),
    newParams: []
  };
  return r;
};

var format = function (output) {
  // TODO: Clean up the output from this so it's similar to all other outputs
  return output;
};

module.exports = function (connectionConfig) {
  var connection = new sqlite.Database(connectionConfig.connection);
  var returnObject = {
    query: function (query, params) {
      return new Promise(function (fulfill, reject) {
        var newParams = convertParams(query, params);
        var newStack = new Error();
        connection.all(newParams.newQuery, newParams.newParams, function (e, r) {
          if (e) {
            var err = new Error(e.message + '\n\terrno: ' + e.errno + '\n\tcode: ' + e.code + '\n');
            err.stack = err.stack + newStack.stack.replace(/^Error\n/, '\n');
            err.errno = e.errno;
            err.code = e.code;
            reject(err);
          } else {
            fulfill(format(r));
          }
        });
      });
    },
    close: connection.close
  };
  return returnObject;
};
