module.exports = function (inArray, field) {
  // This takes an array and returns only the specified field
  // default is "name"
  field = field || 'name';
  return (inArray || []).map(function (item) {
    return typeof item === 'object' ? item[field] : item;
  });
};
