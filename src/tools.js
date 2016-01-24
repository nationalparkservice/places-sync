var fs = require('fs');
var path = require('path');

var tools = module.exports = {
  arrayify: function (value) {
    return Array.isArray(value) ? value : [value];
  },
  readOutput: function (output) {
    return JSON.stringify(tools.arrayify(output).map(function (o) {
      if (Array.isArray(o)) {
        return tools.readOutput(o);
      } else {
        return tools.normalizeTypes(o);
      }
    }), null, 2).replace(/\\n/g, '\n');
  },
  addTitles: function (titles, data) {
    var returnValue = {};
    titles.forEach(function (title, index) {
      returnValue[title] = data[index];
    });
    return returnValue;
  },
  getType: function (value, maxType, dataType) {
    var type = 'text';
    value = value && value.toString ? value.toString() : value;
    if (value && !(isNaN(value) || value.replace(/ /g, '').length < 1) && maxType !== 'text' && dataType !== 'string') {
      type = 'float';
      if (parseFloat(value, 10) === parseInt(value, 10) && maxType !== 'float') {
        type = 'integer';
      }
    }
    return value ? type : maxType;
  },
  requireDirectory: function (directory) {
    var regexp = new RegExp('(.+?)\.js$');
    var returnValue = [];
    fs.readdirSync(directory).forEach(function (file) {
      var match = file.match(regexp);
      if (match) {
        returnValue[match[1]] = require(path.resolve(path.join(directory, file)));
      }
    });
    return returnValue;
  },
  syncPromise: function (fn, throwError) {
    // Converts a synchronous function into a promise
    // This is basically an anti-pattern
    // But it IS useful on homogenizing some functions
    return function () {
      var result;
      var error;
      try {
        result = fn.apply(this, arguments);
      } catch (e) {
        error = e;
      }
      var returnObj = {
        'then': function (thenFn) {
          if (!throwError && !error) {
            thenFn(result);
          }
          return returnObj;
        },
        'catch': function (catchFn) {
          if (throwError) {
            catchFn(result || error);
          } else if (error) {
            catchFn(error);
          }
          return returnObj;
        }
      };
      return returnObj;
    };
  },
  simplifyArray: function (inArray, field) {
    // This takes an array and returns only the specified field
    // default is "name"
    field = field || 'name';
    return (inArray || []).map(function (item) {
      return typeof item === 'object' ? item[field] : item;
    });
  },
  desimplifyArray: function (inArray, field) {
    // This does the reverse of simplifyArray
    // it takes a simple array and makes it an array of objects
    // with the former row value in the selected field
    // default is "name"
    field = field || 'name';
    return (inArray || []).map(function (item) {
      var returnObject = {};
      returnObject[field] = item;
      return typeof item !== 'object' ? returnObject : item;
    });
  },
  buildUrlQuery: function (root, queryObj) {
    var query = [];
    for (var item in queryObj) {
      // The normalize function "normalizes" Boolean fields to 0 or 1, but URLs usually work differently
      if (Object.prototype.toString.call(queryObj[item]).slice(8,-1) === 'Boolean') {
        queryObj[item] = queryObj[item].toString();
      }
      query.push(item + '=' + encodeURIComponent(tools.normalizeTypes(queryObj[item])));
    }
    return root + query.join('&');
  },
  normalizeTypes: function (input) {
    // Convert input into a more usable string
    var type = Object.prototype.toString.call(input).slice(8, -1);
    var transforms = {
      'Array': function (value) {
        return JSON.stringify(value);
      },
      'Object': function (value) {
        return JSON.stringify(value);
      },
      'String': function (value) {
        return value;
      },
      'Date': function (value) {
        return value.toUTCString();
      },
      'Error': function (value) {
        return value.stack || value.toString();
      },
      'RegExp': function (value) {
        return value.toString();
      },
      'Function': function (value) {
        return value.toString();
      },
      'Boolean': function (value) {
        return value ? 1 : 0;
      },
      'Number': function (value) {
        return value;
      },
      'Null': function (value) {
        return null;
      },
      'Undefined': function (value) {
        return null;
      }
    };
    return transforms[type] ? transforms[type](input) : input.toString();
  }
};
