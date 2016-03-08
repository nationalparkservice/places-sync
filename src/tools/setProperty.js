module.exports = function (propName, propValue, baseObj) {
  baseObj = baseObj || {};
  baseObj[propName] = propValue;
  return baseObj;
};
