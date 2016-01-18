var max = 1000000;
var a = 'sdfsd'; //[1, 2, 3, 4, 5];
var tests = 10;

var arrayify = function (value) {
  return Array.isArray(value) ? value : [value];
};

for (var j = 0; j < tests; j++) {
  console.time('concat' + (j + 1));
  for (var i = 0; i < max; i++) {
    [].concat(a);
  }
  console.timeEnd('concat' + (j + 1));
}
for (j = 0; j < tests; j++) {
  console.time('concat' + (j + 1));
  for (i = 0; i < max; i++) {
    a = arrayify(a);
  }
  console.timeEnd('concat' + (j + 1));
}
