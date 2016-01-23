var tools = require('../tools');

module.exports = tools.syncPromise(function (source) {
  // Add the data to the object
  source.data = JSON.parse(typeof source.data === 'string' ? source.data : JSON.stringify(source.data));
  var predefinedColumns = !!source.columns;
  source.columns = [];
  if (typeof source.data === 'string') {
    source.data = JSON.parse(source.data);
  }

  // Go through all the columns and determine their data types
  source.data.forEach(function (row) {
    var thisColumn;
    for (var column in row) {
      if (!predefinedColumns) {
        thisColumn = source.columns.filter(function (value) {
          return value.name === column;
        })[0] || source.columns[source.columns.push({
          'name': column
        }) - 1];
        thisColumn.type = tools.getType(row[column], thisColumn.type || 'integer', typeof row[column]);
      }
    }
  });

  return source;
});
