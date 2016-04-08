var tools = require('../../tools');

module.exports = function (columns, primaryKey, lastUpdatedField, removedField, options) {
  var queryKey = tools.arrayify(primaryKey || tools.simplifyArray(columns));

  var arrayToColumns = function (columns, tableName, quotes, toFrom) {
    quotes = quotes || ['"', '"'];
    quotes[0] = tableName ? quotes[0] + tableName + quotes[1] + '.' + quotes[0] : quotes[0];
    var newColumns = tools.simplifyArray(columns).map(function (column) {
      var newColumn = tools.surroundValues(column, quotes[0], quotes[1]);
      if (options && options.transforms && options.transforms[column] && options.transforms[column][toFrom]) {
        return tools.surroundValues.apply(this, [newColumn].concat(options.transforms[column][toFrom])) + ' AS "' + column + '"';
      } else {
        return newColumn;
      }
    });
    return newColumns.join(', ');
  };

  var arraysToObj = function (keys, values) {
    // Takes two arrays ['a','b','c'], [1,2,3]
    // And makes an object {'a':1,'b':2,'c':3}
    var returnObject = {};
    if (Array.isArray(values)) {
      keys.forEach(function (key, i) {
        returnObject[key] = values[i];
      });
    } else if (typeof values === 'object') {
      returnObject = values;
    }
    return returnObject;
  };

  var createWhereObj = function (keys, values, options) {
    var valuesObj = arraysToObj(keys, values);
    var whereObj = {};

    // If nothing is specified for a value, the default is (null or not null)
    var defaultWhere = (options && options.defaultWhere) || {
      '$or': [{
        '$eq': null
      }, {
        '$ne': null
      }]
    };

    // Add the default value where nothing else is
    keys.forEach(function (pk) {
      whereObj[pk] = valuesObj[pk] || defaultWhere;
    });

    if (removedField) {
      whereObj[removedField] = {
        '$ne': 1 // TODO Use removedValue
      };
    }

    return tools.createWhereClause(whereObj, tools.simplifyArray(columns), options);
  };

  var queries = {
    'selectAllInCache': function (tableName, queryColumns) {
      tableName = tableName || 'all_data';
      queryColumns = queryColumns || columns;
      var selectAllQuery = 'SELECT ' + arrayToColumns(queryColumns, 'all_data') + ' FROM (';
      selectAllQuery += ' SELECT ' + arrayToColumns(queryColumns, 'cached');
      selectAllQuery += ' FROM "cached"';
      selectAllQuery += ' LEFT JOIN "removed" ON ' + queryKey.map(function (pk) {
        return '"removed"."' + pk + '" = "cached"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' LEFT JOIN  "updated" ON ' + queryKey.map(function (pk) {
        return '"updated".' + pk + ' = "cached"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' WHERE';
      selectAllQuery += queryKey.map(function (pk) {
        return '"removed"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' AND ';
      selectAllQuery += queryKey.map(function (pk) {
        return '"updated"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' UNION';
      selectAllQuery += ' SELECT ' + arrayToColumns(queryColumns, 'updated');
      selectAllQuery += ' FROM "updated") AS "all_data"';
      return selectAllQuery;
    },
    'selectLastUpdate': function (tableName) {
      var lastUpdateColumn = arrayToColumns([lastUpdatedField], tableName, undefined, 'from');
      return 'SELECT COALESCE(MAX(' + lastUpdateColumn + '), -1) AS "lastUpdate" FROM "' + tableName + '" ';
    },
    'cleanUpdate': function () {
      return queries.remove('updated');
    },
    'runUpdate': function () {
      return queries.insert('updated');
    },
    'cleanRemove': function () {
      return queries.remove('removed');
    },
    'runRemove': function () {
      return queries.insert('removed');
    },
    'getUpdated': function () {
      return queries.select('updated');
    },
    'getCached': function () {
      return queries.select('cached');
    },
    'getRemoved': function () {
      return queries.select('removed');
    },
    'selectSince': function (tableName, queryColumns) {
      return queries.select(tableName, queryColumns);
    },
    'insert': function (tableName, queryColumns) {
      queryColumns = queryColumns || columns;
      return 'INSERT INTO "' + tableName + '" (' + arrayToColumns(queryColumns) + ') VALUES (' + arrayToColumns(queryColumns, undefined, ['{{', '}}'], 'to') + ')';
    },
    'select': function (tableName, queryColumns) {
      queryColumns = queryColumns || columns;
      return 'SELECT ' + arrayToColumns(queryColumns, tableName, undefined, 'from') + ' FROM "' + tableName + '"';
    },
    'remove': function (tableName) {
      return 'DELETE FROM "' + tableName + '"';
    }
  };
  return function (queryName, values, keys, tableName) {
    var where;
    if (values) {
      if (queryName === 'selectSince') {
        // Special case for the last updated which requires a great than
        where = tools.createWhereClause(tools.setProperty(lastUpdatedField, {
          '$gt': values[lastUpdatedField]
        }), tools.simplifyArray(columns), options);
      } else {
        where = createWhereObj(tools.simplifyArray(keys || queryKey), values, options);
      }
    }
    var query = queries[queryName](tableName, tools.simplifyArray(keys || queryKey)) + (where ? ' WHERE ' + where[0] : ';');

    return [query, where && where[1]];
  };
};
