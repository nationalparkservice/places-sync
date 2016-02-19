var fs = require('fs');
var path = require('path');

var tools = module.exports = {
  arrayify: function (value) {
    return Array.isArray(value) ? value : [value];
  },
  dearrayify: function (value) {
    return Array.isArray(value) && value.length === 1 ? value[0] : value;
  },
  denullify: function (obj, otherRejections) {
    // Removes nulls or undefined from objects
    var newObj = {};
    var rejectTypes = [null, undefined];
    rejectTypes.concat(otherRejections);
    for (var field in obj) {
      if (rejectTypes.indexOf(obj[field]) === -1 && obj.hasOwnProperty(field)) {
        newObj[field] = obj[field];
      }
    }
    return newObj;
  },
  getJsType: function (value) {
    return Object.prototype.toString.call(value).slice(8, -1).toLowerCase();
  },
  dedupe: function (a) {
    // http://stackoverflow.com/questions/9229645/remove-duplicates-from-javascript-array
    var seen = {};
    var out = [];
    var len = a.length;
    var j = 0;
    for (var i = 0; i < len; i++) {
      var item = a[i];
      if (seen[item] !== 1) {
        seen[item] = 1;
        out[j++] = item;
      }
    }
    return out;
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
  readError: function (errorArray, type) {
    // Will look for any type other than an array, since that wouldn't work
    type = (type && type.toLowerCase()) || 'error';
    if (type === 'array') {
      return;
    }

    var inArray = tools.arrayify(errorArray);
    var inputType;
    var subValue;
    for (var i = inArray.length - 1; i >= 0; i--) {
      inputType = tools.getJsType(inArray[i]);
      if (inputType === type) {
        return inArray[i];
      } else if (inputType === 'array') {
        subValue = tools.readError(inArray[i], type);
        if (subValue) {
          return subValue;
        }
      }
    }
    return errorArray;
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
    var regexp = new RegExp('(.+?)\.js(on)?$');
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
        if (tools.getJsType(fn) === 'function') {
          result = fn.apply(this, arguments);
        } else {
          result = function () {
            return fn;
          };
        }
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
      // The normalize function "normalizes" Boolean fields to 0 or 1, but URLs usually use true/false strings
      if (tools.getJsType(queryObj[item]) === 'boolean') {
        queryObj[item] = queryObj[item].toString();
      }
      query.push(item + '=' + encodeURIComponent(tools.normalizeTypes(queryObj[item])));
    }
    return root + query.join('&');
  },
  normalizeTypes: function (input) {
    // Convert input into a more usable string
    var type = tools.getJsType(input);
    var transforms = {
      'array': function (value) {
        return JSON.stringify(value);
      },
      'object': function (value) {
        return JSON.stringify(value);
      },
      'string': function (value) {
        return value;
      },
      'date': function (value) {
        return value.toUTCString();
      },
      'error': function (value) {
        return value.stack || value.toString();
      },
      'regexp': function (value) {
        return value.toString();
      },
      'function': function (value) {
        return value.toString();
      },
      'boolean': function (value) {
        return value ? 1 : 0;
      },
      'number': function (value) {
        return value;
      },
      'null': function (value) {
        return null;
      },
      'undefined': function (value) {
        return null;
      }
    };
    return transforms[type] ? transforms[type](input) : input.toString();
  },
  groupAndRank: function (data, groupBy, rank, returnArrayOf) {
    // If return Arrays is true, it will return only three values in the object
    // { groupBy:, rank: {returnArrayOf}: [] }
    var newArray = [];
    var i, filtered, dups;
    data = JSON.parse(JSON.stringify(data));
    for (i = data.length - 1; i >= 0; i--) {
      if (tools.simplifyArray(newArray, groupBy).indexOf(data[i][groupBy]) === -1) {
        filtered = data.filter(function (a) {
          return a[groupBy] === data[i][groupBy];
        });
        dups = filtered.sort(function (a, b) {
          return a[rank] - b[rank];
        })[0];
        if (returnArrayOf) {
          tools.arrayify(returnArrayOf).map(function (field) {
            dups[field] = filtered.filter(function (value) {
              return value[rank] === dups[rank];
            }).map(function (innerValues) {
              return innerValues[field];
            });
          });
        }
        newArray.push(dups);
      }
    }
    return newArray;
  }
};
