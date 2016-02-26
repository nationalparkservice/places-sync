module.exports = function (obj, otherRejections) {
  // Removes nulls or undefined from objects
  var newObj = {};
  var rejectTypes = [null, undefined];
  rejectTypes.concat(otherRejections);
  for (var field in obj) {
    if (rejectTypes.indexOf(obj[field]) === -1 && obj.hasOwnProperty(field)) {
      newObj[field] = obj[field];
    }
  }
  return newObj;
};
