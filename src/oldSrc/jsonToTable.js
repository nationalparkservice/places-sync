var Bluebird = require('datawrap').Bluebird;
var tools = require('./tools');

module.exports = function (tableName, data, columns) { // TODO: Support external column types
  return new Bluebird(function (fulfill, reject) {
    var jsonInfo = {
      'name': tableName,
      'columns': []
    };
    var types = {};
    try {
      jsonInfo.data = JSON.parse(typeof data === 'string' ? data : JSON.stringify(data));
    } catch (parseError) {
      reject(parseError);
    }

    if (jsonInfo.data) {
      jsonInfo.data.forEach(function (row) {
        for (var column in row) {
          if (jsonInfo.columns.indexOf(column) === -1) {
            jsonInfo.columns.push(column);
            types[column] = types[column] || 'integer';
          }
          types[column] = tools.getType(row[column], types[column], typeof row[column]);
        }
      });
      jsonInfo.columns = jsonInfo.columns.map(function (name) {
        return {
          'name': name,
          'type': types[name]
        };
      });
      fulfill(jsonInfo);
    }
  });
};


