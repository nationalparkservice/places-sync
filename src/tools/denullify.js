var getJsType = require('./getJsType');

var denullify = module.exports = function (obj, recursive, otherRejections) {
  // Removes nulls or undefined from objects
  var newObj = {};
  var rejectTypes = otherRejections || [null, undefined];
  for (var field in obj) {
    if (rejectTypes.indexOf(obj[field]) === -1 && obj.hasOwnProperty(field)) {
      if (recursive && getJsType(obj[field]) === 'object') {
        newObj[field] = denullify(obj[field]);
      } else {
        newObj[field] = obj[field];
      }
    }
  }
  return newObj;
};
