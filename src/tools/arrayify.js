module.exports = function (value) {
  return value !== undefined ? Array.isArray(value) ? value : [value] : [];
};
