module.exports = function (array, before, after) {
  return array.map(function (value) {
    return before + value + after;
  });
};
