var tape = require('tape');
var parse = require('../parseJson.js');
var tests = [{
  name: 'Object',
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
  name: 'JSON String',
  a: '{"test": "one"}',
  res: {
    e: undefined,
    r: {
      test: 'one'
    }
  }
}, {
  name: 'Invalid String',
  a: 'test',
  res: {
    e: 'Value cannot be made into an object',
    r: undefined
  }
}, {
  name: 'Valid Array',
  a: [1, 2],
  res: {
    e: undefined,
    r: {
      '0': 1,
      '1': 2
    }
  }
}, {
  name: 'Valid Array as String',
  a: '[1,2]',
  res: {
    e: undefined,
    r: {
      '0': 1,
      '1': 2
    }
  }
}, {
  name: 'Invalid ATOB encoded Object as String',
  a: 'eyJhIjoxfQ==',
  res: {
    e: undefined,
    r: {
      a: 1
    }
  }
}, {
  name: 'Invalid encoded String',
  a: 'W29iamVjdCBPYmplY3Rd',
  res: {
    e: 'Value cannot be made into an object',
    r: undefined
  }
}, {
  name: 'Valid lz-string compress encoded String',
  a: '㞂₆⁜ࣀ빀',
  res: {
    e: undefined,
    r: {
      a: 1
    }
  }
}, {
  name: 'Valid lz-string compressToBase64 encoded String',
  a: 'N4IghiBcCMC+Q===',
  res: {
    e: undefined,
    r: {
      a: 1
    }
  }
}, /*{
  name: 'Valid lz-string compressToUTF16 encoded String',
  a: '\u1BE1\u0841\u442B\u40AC  ',
  res: {
    e: undefined,
    r: {
      a: 1
    }
  }
}, */{
  name: 'Valid lz-string compressToEncodedURIComponent encoded String',
  a: 'N4IghiBcCMC+Q',
  res: {
    e: undefined,
    r: {
      a: 1
    }
  }
}];

tape('Check parseJson]', function (t) {
  for (var i = 0; i < tests.length; i++) {
    var result = parse(tests[i].a);
    console.log('result for', tests[i].name, '→', result);
    t.deepEqual(result, tests[i].res);
  }
  t.end();
});
