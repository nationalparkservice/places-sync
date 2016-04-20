var mapFields = ['mapped', 'mappedFrom'];
var map = function (columns, mapField) {
  return columns.filter(function (c) {
    // A null mapped field means that we are mapping the field to disappear (basically > /dev/null)
    return c[mapField] !== null;
  }).map(function (c) {
    if (c[mapField] || c[mapField] === 0) {
      c[mapFields[(mapFields.indexOf(mapField) + 1) % mapFields.length]] = c.name;
      c.name = c[mapField];
    }
    return c;
  });
};

module.exports = {
  'to': function (columns) {
    console.log('to');
    return map(columns, mapFields[0]);
  },
  'from': function (columns) {
    console.log('from');
    return map(columns, mapFields[1]);
  }
};
