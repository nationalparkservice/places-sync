var tools = module.exports = {
  arrayify: function(value) {
    return Array.isArray(value) ? value : [value];
  },
  readOutput: function(output) {
    return JSON.stringify(tools.arrayify(output).map(function(o) {
      if (o.toString().substr(0, 5) === 'Error') {
        return o.toString + '\\n' + o.stack;
      } else {
        return JSON.stringify(o, null, 2);
      }
    }), null, 2).replace(/\\n/g, '\n');
  },
  addTitles: function(titles, data) {
    var returnValue = {};
    titles.forEach(function(title, index) {
      returnValue[title] = data[index];
    });
    return returnValue;
  },
  getType: function(value, maxType, dataType) {
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
  normalizeTypes: function(input) {
    // Convert input into a more usable string
    var type = Object.prototype.toString.call(input).slice(8, -1);
    var transforms = {
      'Array': function(value) {
        return JSON.stringify(value);
      },
      'Object': function(value) {
        return JSON.stringify(value);
      },
      'String': function(value) {
        return value;
      },
      'Date': function(value) {
        return value.toUTCString();
      },
      'Error': function(value) {
        return value.stack || value.toString();
      },
      'RegExp': function(value) {
        return value.toString();
      },
      'Function': function(value) {
        return value.toString();
      },
      'Boolean': function(value) {
        return value ? 1 : 0;
      },
      'Number': function(value) {
        return value;
      },
      'Null': function(value) {
        return null;
      },
      'Undefined': function(value) {
        return null;
      }
    };
    return transforms[type] ? transforms[type](input) : input.toString();
  };
};
