var simplifyArray = require('../../tools/simplifyArray');
var setProperty = require('../../tools/setProperty');
var arrayify = require('../../tools/arrayify');
var valueExists = function (v) {
  // We use index of 0 to show that things exist, so we can't use falsey
  return !(v === undefined || v === null || v === false);
};

var mapFields = function (fields, mappings) {
  // This isn't used anywhere
  fields = JSON.parse(JSON.stringify(fields));
  var isArray, newField;
  for (var field in fields) {
    isArray = Array.isArray(fields[field]);
    newField = arrayify(fields[field]).map(function (c) {
      return mappings && mappings[c] !== false ? mappings[c] : c;
    }).filter(function (c) {
      valueExists(c);
    });
    if (!isArray) {
      fields[field] = newField[0];
    } else {
      fields[field] = newField;
    }
  }
  return fields;
};

var valueField = function (columns, field) {
  var possibleColumns = columns.filter(function (c) {
    return c.mapped !== false;
  }).map(function (c) {
    return setProperty(c.name, c[field]);
  });
  if (possibleColumns.length > 0) {
    return possibleColumns.reduce(function (a, b) {
      for (var k in b) {
        a[k] = b[k];
      }
      return a;
    });
  } else {
    return undefined;
  }
};
module.exports = function (columns, mapKeyFields) {
  var returnValue = {
    'all': simplifyArray(columns),
    'primaryKeys': simplifyArray(columns.filter(function (c) {
      return valueExists(c.primaryKey);
    })),
    'foreignKeys': simplifyArray(columns.filter(function (c) {
      return valueExists(c.foreignKey);
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
    'dataField': simplifyArray(columns.filter(function (c) {
      return valueExists(c.data);
    }))[0],
    'removedValue': ((columns.filter(function (c) {
      return valueExists(c.removed);
    })[0]) || {}).removedValue || 'true',
    'mapped': valueField(columns, 'mapped'),
    'mappedFrom': valueField(columns, 'mappedFrom')
  };
  if (mapKeyFields) {
    returnValue = mapFields(returnValue, returnValue.mapFields);
  }
  return returnValue;
};
