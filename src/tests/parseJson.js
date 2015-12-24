var tape = require('tape');
var parse = require('../parseJson.js');
var tests = [{
  a: {
    'test': 'one'
  },
  res: {
    e: undefined,
    r: {
      test: 'one'
    }
  }
}, {
  a: '{"test": "one"}',
  res: {
    e: undefined,
    r: {
      test: 'one'
    }
  }
}, {
  a: 'test',
  res: {
    e: new Error('SyntaxError: Unexpected token µ'),
    r: undefined
  }
}, {
  a: [1, 2],
  res: {
    e: undefined,
    r: {
      '0': 1,
      '1': 2
    }
  }
}, {
  a: '[1,2]',
  res: {
    e: undefined,
    r: {
      '0': 1,
      '1': 2
    }
  }
}, {
  a: 'eyJhIjoxfQ==',
  res: {
    e: undefined,
    r: {
      a: 1
    }
  }
}, {
  a: 'W29iamVjdCBPYmplY3Rd',
  res: {
    e: new Error('SyntaxError: Unexpected token o'),
    r: undefined
  }
}];

tape('Check parseJson]', function (t) {
  for (var i = 0; i < tests.length; i++) {
    var result = parse(tests[i].a);
    console.log('result', result);
    t.deepEqual(result, tests[i].res);
  }
  t.end();
});
