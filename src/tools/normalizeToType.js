var getJsType = require('./getJsType');

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
  },
  'default': function (value) {
    return value.toString();
  }
};

module.exports = function (input, type) {
  // Convert input into a more usable string
  type = type || getJsType(input);
  return transforms[transforms[type] ? type : 'default'](input);
};
