var tape = require('tape');
var guid = require('../guid.js');

var tests = [{
  name: 'number',
  a: 5,
  res: 'e4da3b7f-bbce-2345-d777-2b0674a318d5'
}, {
  name: 'text',
  a: 'text',
  res: '1cb251ec-0d56-8de6-a929-b520c4aed8d1'
}, {
  name: 'random input',
  a: Math.random().toString(35).substr(-25),
  res: function (d) {
    return (d.length = 36 && d.match(/-/g).length === 4 && (d[8] + d[13] + d[18] + d[23]) === '----');
  }
}, {
  name: 'blank',
  a: undefined,
  res: function (d) {
    return (d.length = 36 && d.match(/-/g).length === 4 && (d[8] + d[13] + d[18] + d[23]) === '----');
  }
}];

var fn = guid;

tape('Check guids and md5', function (t) {
  for (var i = 0; i < tests.length; i++) {
    var result = fn(tests[i].a);
    console.log('result for', tests[i].name, '→', result);
    if (typeof tests[i].res === 'function') {
      t.deepEqual(true, tests[i].res(result));
    } else {
      t.deepEqual(result, tests[i].res);
    }
  }
  t.end();
});
