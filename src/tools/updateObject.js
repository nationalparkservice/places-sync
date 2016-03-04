module.exports = function (baseObject, newValues) {
  var key;
  var newObj = {};
  for (key in baseObject) {
    newObj[key] = newValues[key] !== undefined ? newValues[key] : baseObject[key];
  }
  return newObj;
};
