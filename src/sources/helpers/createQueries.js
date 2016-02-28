var tools = require('../../tools');

module.exports = function (columns, primaryKey, lastUpdate) {
  var queryKey = tools.arrayify(primaryKey || tools.simplifyArray(columns));

  var arrayToColumns = function (columns, tableName) {
    return tools.simplifyArray(columns).map(function (c) {
      return (tableName ? '"' + tableName + '".' : '') + '"' + c + '"';
    }).join(',');
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

    return tools.createWhereClause(whereObj);
  };

  var queries = {
    'selectAll': function () {
      var selectAllQuery = 'SELECT ' + arrayToColumns(columns, 'all_data') + ' FROM (';
      selectAllQuery += ' SELECT ' + arrayToColumns(columns, 'source');
      selectAllQuery += ' FROM "source"';
      selectAllQuery += ' LEFT JOIN "remove" ON ' + queryKey.map(function (pk) {
        return '"remove"."' + pk + '" = "source"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' LEFT JOIN  "new" ON ' + queryKey.map(function (pk) {
        return '"new".' + pk + ' = "source"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' WHERE';
      selectAllQuery += queryKey.map(function (pk) {
        return '"remove"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' AND ';
      selectAllQuery += queryKey.map(function (pk) {
        return '"new"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' UNION';
      selectAllQuery += ' SELECT ' + arrayToColumns(columns, 'new');
      selectAllQuery += ' FROM "new") AS "all_data"';
      return selectAllQuery;
    },
    'selectLastUpdate': function () {
      if (lastUpdate) {
        return 'SELECT MAX("all_data"."' + lastUpdate + '" AS "lastUpdate") FROM ' + queries.selectAllQuery + ') AS "last_update"';
      } else {
        return 'SELECT 0 AS "lastUpdate" ';
      }
    }
  };
  return function (queryName, values, keys) {
    var where = createWhereObj(tools.simplifyArray(keys || queryKey), values);
    var query = queries[queryName]() + ' WHERE ' + where[0];
    return [query, where[1]];
  };
};
