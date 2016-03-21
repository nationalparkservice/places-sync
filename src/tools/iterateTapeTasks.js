var Promise = require('bluebird');
var chalk = require('chalk');
var dedupe = require('./dedupe');
var fandlebars = require('fandlebars');
var getJsType = require('./getJsType');
var iterateTasks = require('./iterateTasks');
var stringify2 = require('json-stringify-pretty-compact');
var tape = require('tape');
var arrayify = require('./arrayify');
Promise.config({
  longStackTraces: true
});
var stringify = function (v) {
  var str = stringify2(v);
  if (str !== undefined && str !== null && str.toString) {
    return str.toString();
  } else {
    return typeof str;
  }
};

var testObjStructure = function (result, expected, msg, t) {
  var readObj = function (obj, shouldHave) {
    for (var name in shouldHave) {
      if (obj[name] === undefined && shouldHave[name] !== undefined) {
        return name;
      } else if (typeof shouldHave[name] === 'object') {
        var returnValue = readObj(obj[name], shouldHave[name]);
        if (returnValue !== false) {
          return name + '.' + returnValue;
        }
      }
    }
    return false;
  };
  if (typeof result !== 'object') {
    return t.fail(msg);
  } else {
    var checkObj = readObj(result, expected);
    if (checkObj === false) {
      return t.pass(msg);
    } else {
      return t.equal(undefined, checkObj, msg);
    }
  }
};

var runTests = function (tests, results, showResult) {
  var report = [];
  return new Promise(function (fulfill, reject) {
    return tape('sync test list', function (t) {
      tests.forEach(function (test, i) {
        var expected = test.expected;
        var result = results[i];
        try {
          if (typeof expected === 'string' && expected.match(/^\{\{.+?\}\}/g)) {
            expected = fandlebars(expected, results, null, true)[expected];
          }
          report.push({
            'name': test.name,
            'result': result,
            'operator': test.operator || 'deepEqual',
            'expected': expected
          });
          var msg = '([' + test.operator + '] ' + test.name + ')';
          var time = parseInt(results._iterateTasksStats ? results._iterateTasksStats[i] : '0', 10);
          var rank = (dedupe(results._iterateTasksStats || []).filter(function (t) {
            return time < parseInt(t, 10);
          }).length + 1) / dedupe(results._iterateTasksStats || []).length;
          var color = rank > (2 / 3) ? 'green' : (rank > (1 / 3) ? 'yellow' : 'red');
          console.log('╔══════════════════════════════════════╗');
          console.log('║ Test:', chalk.cyan(test.name), '(' + chalk[color](time.toString() + ' ms)'));
          if (test.description) {
            console.log('║ Description:', chalk.cyan(test.description));
          }
          console.log('╟──────────────────────────────────────');
          process.stdout.write('║ ');
          if (test.operator === 'structureEqual') {
            // We have our own test that just makes sure the structure is equal
            testObjStructure(result, expected, msg, t);
          } else if (test.operator === 'jstype') {
            t.equal(getJsType(result), expected, msg);
          } else if (test.operator === 'custom' && test.customTest) {
            t.equal(test.customTest(result), expected, msg);
          } else {
            t[test.operator || 'deepEqual'](result, expected, msg);
          }
          if (showResult) {
            console.log('╟──────────────────────────────────────');
            process.stdout.write('║ ');
            console.log(stringify(result).split('\n').join('\n║ '));
          }
          console.log('╚══════════════════════════════════════╝');
        } catch (e) {
          console.log(e.stack);
          reject(e);
        }
      });
      report.time = results._iterateTasksStats ? results._iterateTasksStats.reduce(function (a, b) {
        return parseInt(a, 10) + parseInt(b, 10);
      }) : 0;
      fulfill(report);
      t.end();
    });
  });
};

module.exports = function (tests, breakOnError, verboseTasks, showResult) {
  var error;
  return new Promise(function (fulfill, reject) {
    (iterateTasks(tests, 'Iterate Tape Tasks', verboseTasks, false).then(function (results) {
      return runTests(tests, results, showResult);
    }).catch(function (results) {
      error = getJsType(results[results.length - 1]) === 'error' ? results[results.length - 1] : getJsType(results) === 'error' ? results : new Error(results);
      if (breakOnError) {
        reject(error);
      } else {
        return runTests(tests, results, showResult);
      }
    })).then(function (report) {
      console.log('Cumulative Task Time: ', report ? report.time : 'unknown due to error');
      if (error !== undefined) {
        reject(error);
      } else {
        fulfill(report);
      }
    });
  });
};
