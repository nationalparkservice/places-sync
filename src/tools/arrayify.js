module.exports = function (value) {
  return Array.isArray(value) ? value : [value];
};
