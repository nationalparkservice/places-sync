var tools = require('./tools');

module.exports = function (tableName, data, predefinedColumns) {
  // Define the new info object
  var jsonInfo = {
    'name': tableName,
    'columns': predefinedColumns || []
  };

  // Add the data to the object
  jsonInfo.data = JSON.parse(typeof data === 'string' ? data : JSON.stringify(data));

  // Go through all the columns and determine their data types
  jsonInfo.data.forEach(function (row) {
    var thisColumn;
    for (var column in row) {
      if (!predefinedColumns) {
        thisColumn = jsonInfo.columns.filter(function (value) {
            return value.name === column;
          })[0] || jsonInfo.columns[jsonInfo.columns.push({
            'name': column
          }) - 1];
        thisColumn.type = tools.getType(row[column], thisColumn.type || 'integer', typeof row[column]);
      }
    }
  });
  return jsonInfo;
};
