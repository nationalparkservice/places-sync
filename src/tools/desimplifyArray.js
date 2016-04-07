module.exports = function (inArray, field) {
  // This does the reverse of simplifyArray
  // it takes a simple array and makes it an array of objects
  // with the former row value in the selected field
  // default is "name"
  field = field || 'name';
  return (inArray || []).map(function (item) {
    var returnObject = {};
    returnObject[field] = item;
    return typeof item !== 'object' ? returnObject : item;
  });
};
