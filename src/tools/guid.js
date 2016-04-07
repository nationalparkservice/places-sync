// Makes a guid in the method postgres uses (with an md5)
// http://stackoverflow.com/questions/12505158/generating-a-uuid-in-postgres-for-insert-statement
// SELECT md5(random()::text || clock_timestamp()::text)::uuid
// seed of 5 should = e4da3b7f-bbce-2345-d777-2b0674a318d5

var md5 = require('./md5');
module.exports = function(seed) {
  var seed = md5((seed || Math.random().toString() + new Date().getTime().toString()).toString());
  var format = '00000000-0000-0000-0000-000000000000';
  var returnValue = '';
  var position = 0;
  for (var charIndex = 0; charIndex < format.length; charIndex++) {
      returnValue += format[charIndex] !== '0' ? format[charIndex] : seed[position];
      position += format[charIndex] !== '0' ? 0 : 1;
  }
  return returnValue;
};

