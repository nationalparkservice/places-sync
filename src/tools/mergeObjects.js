module.exports = function () {
  var newObject = {};
  var objs = [];
  for (var arg in arguments) {
    if (typeof arguments[arg] === 'object') {
      objs.push(arguments[arg]);
    }
  }
  var field;
  for (var i = objs.length; i >= 0; i--) {
    for (field in objs[i]) {
      newObject[field] = objs[i][field];
    }
  }
  if (Object.keys(newObject).length === 0) {
    newObject = undefined;
  }
  return newObject;
};
