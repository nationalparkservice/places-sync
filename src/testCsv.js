// Load that csv
var sources = require('./sources');
var iterateTasks = require('./tools/iterateTasks');

var csvConfig = {
  'connection': {
    'filePath': __dirname + '/' + 'test.csv',
    'type': 'csv'
  },
  'lastUpdateField': null,
  'removedField': null,
  'primaryKey': ['a', 'b']
};
var thrower = function (e) {
  throw Array.isArray(e) ? e[e.length = 1] : e;
};

sources(csvConfig).then(function (source) {
  var tasks = [{
    'name': 'selectAll',
    'task': source.cache.selectAll,
    'params': []
  }, {
    'name': 'selectLastUpdate',
    'task': source.selectLastUpdate,
    'params': []
  }, {
    'name': 'describe',
    'task': source.describe,
    'params': []
  }, {
    'name': 'create',
    'task': source.modify.create,
    'params': [{
      'a': 10,
      'b': 11,
      'c': 12,
      'd': 13,
      'e': 14,
      'f': 'fifteen'
    }]
  }, {
    'name': 'selectAll',
    'task': source.cache.selectAll,
    'params': []
  }, {
    'name': 'remove',
    'task': source.modify.remove,
    'params': [{
      'a': 10,
      'b': 11
    }]
  }, {
    'name': 'selectAll',
    'task': source.cache.selectAll,
    'params': []
  }, {
    'name': 'create',
    'task': source.modify.create,
    'params': [{
      'a': 10,
      'b': 11,
      'c': 112,
      'd': 113,
      'e': 114,
      'f': 'one hundred fifteen'
    }]
  }, {
    'name': 'selectAll',
    'task': source.cache.selectAll,
    'params': []
  }, {
    'name': 'update',
    'task': source.modify.update,
    'params': [{
      'a': 10,
      'b': 11,
      'c': 2,
      'd': 3,
      'e': 4,
      'f': 'five'
    }]
  }, {
    'name': 'selectAll',
    'task': source.cache.selectAll,
    'params': []
  }, {
    'name': 'write out',
    'task': source.save,
    'params': []
  }, {
    'name': 'close',
    'task': source.close,
    'params': []
  }];

  return iterateTasks(tasks, 'test', true);
}).then(console.log).catch(thrower);
