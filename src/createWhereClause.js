var md5 = require('./md5');
var tools = require('./tools');

module.exports = function (whereObj, availableColumns) {
  var whereReplacers = {};
  var operators = {
    'eq': '=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'ne': '!=',
    'like': 'LIKE'
  };
  var join = function (statementsToJoin, orJoin) {
    var joiner = orJoin ? 'OR' : 'AND';
    statementsToJoin = statementsToJoin.filter(function (s) {
      return s && s[0];
    });
    return statementsToJoin.length ? '(' + statementsToJoin.join(' ' + joiner + ' ') + ')' : undefined;
  };
  var createStatement = function (field, operator, value) {
    var valueMd5 = md5(tools.normalizeTypes(value) + tools.getJsType(value));
    whereReplacers[valueMd5] = value;
    if (!availableColumns || availableColumns.indexOf(field) > -1) {
      return '"' + field + '" ' + operator + ' {{' + valueMd5 + '}}';
    } else {
      return undefined;
    }
  };

  var addStatements = function (innerWhereObj) {
    var statements = [];
    var tmp = {};
    if (tools.getJsType(innerWhereObj) === 'object') {
      for (var field in innerWhereObj) {
        if (field === '$and' || field === '$or') {
          if (Array.isArray(innerWhereObj[field])) {
            statements.push(join(innerWhereObj[field].map(function (secondWhereObj) {
              return addStatements(secondWhereObj);
            }), field === '$or'));
          }
        } else if (Array.isArray(innerWhereObj[field])) {
          // We assume everything in an array uses an equal operator, but gets joined by ors
          statements.push(join(innerWhereObj[field].map(function (value) {
            return createStatement(field, operators.eq, value);
          }), true));
        } else if (typeof innerWhereObj[field] === 'object') {
          for (var operator in innerWhereObj[field]) {
            if (operators[operator.slice(1)]) {
              statements.push(createStatement(field, operators[operator.slice(1)], innerWhereObj[field][operator]));
            } else if (operator === '$and' || operator === '$or') {
              tmp = {};
              if (Array.isArray(innerWhereObj[field][operator])) {
                tmp[operator] = innerWhereObj[field][operator].map(function (secondWhereObj) {
                  var tmp2 = {};
                  tmp2[field] = secondWhereObj;
                  return tmp2;
                });
                statements.push(addStatements(tmp));
              }
            }
          }
        } else {
          statements.push(createStatement(field, operators.eq, innerWhereObj[field]));
        }
      }
    }
    return statements;
  };

  var finalStatements = addStatements(whereObj);
  return finalStatements.length ? [join(finalStatements), whereReplacers] : [undefined, whereReplacers];
};
