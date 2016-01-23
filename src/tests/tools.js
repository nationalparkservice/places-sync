ar tape = require('tape');
var tools = require('../tools');

var tests = [{
  'name': 'arrayify',
  'task': tools.arrayify,
  'params': ['a'],
  'expected': ['a']
}, {
  'name': 'arrayify',
  'task': tools.arrayify,
  'params': [
    ['a']
  ],
  'expected': ['a']
}, {
  'name': 'arrayify',
  'task': tools.arrayify,
  'params': [
    ['a', 1, 2, 3, 'b']
  ],
  'expected': ['a', 1, 2, 3, 'b']
}, {
  'name': 'add titles',
  'task': tools.addTitles,
  'params': [
    ['a', 'b', 'c'],
    [0, 1, 2]
  ],
  'expected': {
    'a': 0,
    'b': 1,
    'c': 2
  }
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['1', 'text'],
  'expected': 'text'
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['1', 'float'],
  'expected': 'float'
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['1', 'integer'],
  'expected': 'integer'
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['1.1', 'integer'],
  'expected': 'float'
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['3456345'],
  'expected': 'integer'
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['dfads', 'integer'],
  'expected': 'text'
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['dfadsiadfa', 'float'],
  'expected': 'text'
}, {
  'name': 'getType',
  'task': tools.getType,
  'params': ['dfadsiadfa', 'float'],
  'expected': 'text'
}];

tape('Test the tools js file', function(t) {
  for (var i = 0; i < tests.length; i++) {
    var result = tests[i].task.apply(this, tests[i].params);
    t.deepEqual(result, tests[i].expected);
  }
  t.end();
});
