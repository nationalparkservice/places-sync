var merge = require('../compareJson');

var tests = [{
  a: {},
  b: {},
  c: {}
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  b: {
    'val1': 'changedInB',
    'val2': 'two',
    'val3': 'three'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }
}, {
  a: {
    'val1': 'changedinA',
    'val2': 'two',
    'val3': 'three'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  c: {
    'val1': 'changedInO',
    'val2': 'two',
    'val3': 'three'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'changedInA',
    'val3': 'three'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'changedInB'
  },
  c: {
    'val1': 'changedInO',
    'val2': 'two',
    'val3': 'three'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three',
    'val4': 'four'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }
}, {
  a: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'changedInA'
  },
  b: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'ChangedInB'
  },
  c: {
    'val1': 'one',
    'val2': 'two',
    'val3': 'three'
  }
}];

for (var i = 0; i < tests.length; i++) {
  console.log(merge(tests[i].a, tests[i].b, tests[i].c));
}
