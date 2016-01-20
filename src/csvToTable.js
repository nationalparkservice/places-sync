var Bluebird = require('datawrap').Bluebird;
var csv = require('csv');
var tools = require('./tools');

module.exports = function (tableName, data) {
  return new Bluebird(function (fulfill, reject) {
    csv.parse(data, function (e, r) {
      if (!e) {
        var csvInfo = {
          'name': tableName,
          'data': r.slice(1)
        };
        var types = r[0].map(function () {
          return 'integer';
        });

        csvInfo.data.forEach(function (row) {
          row.forEach(function (column, index) {
            types[index] = tools.getType(column, types[index]);
          });
        });

        csvInfo.columns = r[0].map(function (name, index) {
          return {
            'name': name,
            'type': types[index]
          };
        });

        fulfill(csvInfo);
      } else {
        e.name = 'Cannot parse CSV data ' + e.name;
        reject(e);
      }
    });
  });
};
