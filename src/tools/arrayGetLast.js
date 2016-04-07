var arrayify = require('./arrayify');
module.exports = function (array, first) {
  // Returns the last value in an array
  //   ex. [0,1,2,3] => 3
  // is first is specified, it will return the first value in the array
  //   ex. [0,1,2,3] => 0

  array = arrayify(array);
  var index = first ? 0 : array.length - 1;
  return array[index];
};
