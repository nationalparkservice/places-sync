var getJsType = require('./getJsType');
var md5 = require('./md5');
var normalizeToType = require('./normalizeToType');
var surroundValues = require('./surroundValues');

module.exports = function (whereObj, availableColumns, options) {
  var whereReplacers = {};
  var operators = {
    'eq': '=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'ne': '!=',
    'eqNull': 'IS',
    'neNull': 'IS NOT',
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
    var valueMd5 = md5(normalizeToType(value) + getJsType(value));
    whereReplacers[valueMd5] = value;
    if (value === null) {
      if (operator === '=') {
        operator = 'IS';
      } else if (operator === '!=') {
        operator = 'IS NOT';
      }
    }
    if (!availableColumns || availableColumns.indexOf(field) > -1) {
      var fieldVal = surroundValues(field, '"');
      if (options && options.transforms && options.transforms[field] && options.transforms[field].from) {
        // Allow transformations (like casts or upper/lower kind of things)
        fieldVal = surroundValues.apply(this, [fieldVal].concat(options.transforms[field].from));
      }
      return fieldVal + surroundValues(operator, ' ') + surroundValues(valueMd5, '{{', '}}');
    } else {
      return undefined;
    }
  };

  var addStatements = function (innerWhereObj) {
    var statements = [];
    var tmp = {};
    if (getJsType(innerWhereObj) === 'object') {
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
              } else if (typeof innerWhereObj[field][operator] === 'object') {
                tmp = [];
                for (var secondWhereObjIdx in innerWhereObj[field][operator]) {
                  var tmp2 = {};
                  var tmp3 = {};
                  tmp3[secondWhereObjIdx] = innerWhereObj[field][operator][secondWhereObjIdx];
                  tmp2[field] = tmp3;
                  tmp.push(tmp2);
                }
                tmp = {
                  '$or': tmp
                };
              }
              statements.push(addStatements(tmp));
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
