module.exports = function (array, before, after) {
  after = after || before; // If it's the same character, it only needs to be passed in once
  var isArray = Array.isArray(array);
  if (!isArray) {
    array = [array];
  }
  var returnValue = array.map(function (value) {
    return before + value + after;
  });
  return isArray ? returnValue : returnValue[0];
};
