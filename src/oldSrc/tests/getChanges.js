var getChanges = require('../getChanges');
var tape = require('tape');

var tests = [{
  'name': 'Blank Values, should be no changes',
  'task': getChanges,
  'params': [{}, {}, {}],
  'expected': {
    newValues: {},
    conflicts: []
  }
}, {
  'name': 'Same Values, should be no changes',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'two',
      val3: 'three'
    },
    conflicts: []
  }

}, {
  'name': 'Change in B, should take change from B',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'changedInB',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'changedInB',
      val2: 'two',
      val3: 'three'
    },
    conflicts: []
  }
}, {
  'name': 'Change in A, Should take change from A',
  'task': getChanges,
  'params': [{
    'val1': 'changedinA',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'changedinA',
      val2: 'two',
      val3: 'three'
    },
    conflicts: []
  }

}, {
  'name': 'Change in C, this should show up as a change from A and B that is the same, so it should take the value from A',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'changedInO',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'two',
      val3: 'three'
    },
    conflicts: []
  }

}, {
  'name': 'Change in A and B, val 1 should be what A&B have, val2 and 3 should take the changes made in A and B',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'changedInA',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'changedInB'
  }, {
    'val1': 'changedInO',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'changedInA',
      val3: 'changedInB'
    },
    conflicts: []
  }
}, {
  'name': 'Change Add extra column in B, should add data from B to C',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'two',
      val3: 'three',
      val4: 'four'
    },
    conflicts: []
  }
}, {
  'name': 'Change Remove extra column from A and B, should remove it from C as well',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'two',
      val3: 'three',
      val4: undefined
    },
    conflicts: []
  }
}, {
  'name': 'Remove extra column B, remove data from for that column',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'two',
      val3: 'three',
      val4: undefined
    },
    conflicts: []
  }

}, {
  'name': 'Add extra column from A and B, add column to C',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'two',
      val3: 'three',
      val4: 'four'
    },
    conflicts: []
  }
}, {
  'name': 'Conflict in A and B, take value from A, but report conflict',
  'task': getChanges,
  'params': [{
    'val1': 'one',
    'val2': 'two',
    'val3': 'changedInA'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'ChangedInB'
  }, {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }],
  'expected': {
    newValues: {
      val1: 'one',
      val2: 'two',
      val3: 'changedInA'
    },
    conflicts: ['val3']
  }
}];

tape('Check getChanges', function (t) {
  for (var i = 0; i < tests.length; i++) {
    var result = tests[i].task.apply(this, tests[i].params);
    t.deepEqual(result, tests[i].expected);
  }
  t.end();
});
