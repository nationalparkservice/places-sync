// Takes and array [0,1,2,3] and an array of names ['zero', 'one', 'two', 'three']
// And returns an object ('zero': 0, 'one': 1, 'two': 2, 'three': 3}

module.exports = function (array, names) {
  var returnObject = {};
  names.forEach(function (name, index) {
    returnObject[name] = array[index];
  });
  return returnObject;
};
