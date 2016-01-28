var Bluebird = require('datawrap').Bluebird;
var csv = require('csv');
var tools = require('../tools');

module.exports = function (source) {
  return new Bluebird(function (fulfill, reject) {
    csv.parse(source.data, function (e, r) {
      var csvInfo = {
        'name': source.name
      };
      var types = [];

      if (!e) {
        csvInfo.data = r.slice(1);
        types = r[0].map(function () {
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

        source.data = csvInfo.data;
        source.columns = csvInfo.columns;

        fulfill(source);
      } else {
        e.name = 'Cannot parse CSV data ' + e.name;
        reject(e);
      }
    });
  });
};
