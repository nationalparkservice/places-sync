var simplifyArray = require('../../tools/simplifyArray');
var valueExists = function (v) {
  // We use index of 0 to show that things exist, so we can't use falsey
  return !(v === undefined || v === null || v === false);
};
module.exports = function (columns) {
  var returnValue = {
    'all': simplifyArray(columns),
    'primaryKeys': simplifyArray(columns.filter(function (c) {
      return valueExists(c.primaryKey);
    })),
    'notNullFields': simplifyArray(columns.filter(function (c) {
      return valueExists(c.notNull);
    })),
    'lastUpdatedField': simplifyArray(columns.filter(function (c) {
      return valueExists(c.lastUpdated);
    }))[0],
    'removedField': simplifyArray(columns.filter(function (c) {
      return valueExists(c.removed);
    }))[0],
    'forcedField': simplifyArray(columns.filter(function (c) {
      return valueExists(c.forced);
    }))[0],
    'hashField': simplifyArray(columns.filter(function (c) {
      return valueExists(c.hash);
    }))[0],
    'removedValue': ((columns.filter(function (c) {
      return valueExists(c.removed);
    })[0]) || {}).removedValue || 'true'
  };
  return returnValue;
};
