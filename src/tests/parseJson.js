var parse = require('../parseJson.js');
var tests = [{
  a: {
    'test': 'one'
  }
}, {
  a: '{"test": "one"}'
}, {
  a: 'test'
}, {
  a: [1, 2]
}, {
  a: '[1,2]'
}, {
  a: 'eyJhIjoxfQ=='
}, {
  a: 'W29iamVjdCBPYmplY3Rd'
}];
for (var i=0; i < tests.length; i++) {
  console.log(parse(tests[i]['a']));
}
