var tools = require('../../tools');

module.exports = function (columns, primaryKey, lastUpdatedField, removedField) {
  var queryKey = tools.arrayify(primaryKey || tools.simplifyArray(columns));

  var arrayToColumns = function (columns, tableName, quotes) {
    quotes = quotes || ['"', '"'];
    quotes[0] = tableName ? quotes[0] + tableName + quotes[1] + '.' + quotes[0] : quotes[0];
    return tools.surroundValues(tools.simplifyArray(columns), quotes[0], quotes[1]).join(',');
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

  var createWhereObj = function (keys, values, defaultWhere) {
    var valuesObj = arraysToObj(keys, values);
    var whereObj = {};

    // If nothing is specified for a value, the default is (null or not null)
    defaultWhere = defaultWhere || {
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

    return tools.createWhereClause(whereObj);
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
      return 'SELECT COALESCE(MAX("' + tableName + '"."' + lastUpdatedField + '"), -1) AS "lastUpdate" FROM "' + tableName + '" ';
    },
    'cleanUpdate': function () {
      return queries.removed('updated');
    },
    'runUpdate': function () {
      return queries.insert('updated');
    },
    'cleanRemove': function () {
      return queries.removed('removed');
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
      return 'INSERT INTO "' + tableName + '" (' + arrayToColumns(queryColumns) + ') VALUES (' + arrayToColumns(queryColumns, undefined, ['{{', '}}']) + ')';
    },
    'select': function (tableName, queryColumns) {
      queryColumns = queryColumns || columns;
      return 'SELECT ' + arrayToColumns(queryColumns, tableName) + ' FROM "' + tableName + '"';
    },
    'remove': function (tableName) {
      return 'DELETE FROM "' + tableName + '"';
    }
  };
  return function (queryName, values, keys, tableName) {
    var where = values ? createWhereObj(tools.simplifyArray(keys || queryKey), values) : undefined;
    var query = queries[queryName](tableName, tools.simplifyArray(keys || queryKey)) + (where ? ' WHERE ' + where[0] : ';');

    // Special case for the last updated which requires a great than
    if (queryName === 'selectSince') {
      query = queries['select'](tableName) + ' WHERE "' + lastUpdatedField + '" > ' + values[lastUpdatedField] + ';';
    }
    return [query, where && where[1]];
  };
};
