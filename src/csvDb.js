var datawrap = require('datawrap');
var fs = datawrap.Bluebird.promisifyAll(require('fs'));
var csv = require('csv');
var tools = require('./tools');

module.exports = function (config, defaults, options) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var db = datawrap({
      'type': 'sqlite',
      'name': config.name,
      'connection': ':memory:'
    }, defaults);
    options = options || {};

    var regex = new RegExp('^' + (options.fileDesignator || config.fileDesignator || defaults.fileDesignator));
    var fileOptions = options.fileOptions || config.fileOptions || defaults.fileOptions;
    var csvDirectory = (options.csvDirectory || config.csvDirectory || defaults.csvDirectory);
    csvDirectory = csvDirectory + (csvDirectory.substr(-1) === '/' ? '' : '/');
    var inData = config.data;

    getData(inData, csvDirectory, fileOptions, regex)
      .then(function (d) {
        // Go through the data and add it to the database
        var taskList = d.map(function (table) {
          return {
            'name': 'Create ' + table.name,
            'task': createSqlDatabase,
            'params': [table.name, table.columns, table.data, db]
          };
        });
        datawrap.runList(taskList)
          .then(function (r) {
            fulfill({result: r, database: db});
          }).catch(function (e) {
            reject(e);
          });
      }).catch(function (e) {
        reject(e);
      });
  });
};

var getData = function (inData, csvDirectory, fileOptions, regex) {
  var arrayInData = [];
  for (var record in inData) {
    arrayInData.push({
      'name': record,
      'data': inData[record]
    });
  }
  return datawrap.runList(arrayInData.map(function (d, i) {
    if (d.data.match(regex)) {
      // is a file
      return {
        'name': 'Read file: ' + d.name,
        'task': readCsvFile,
        'params': [d.name, d.data.replace(regex, csvDirectory), fileOptions]
      };
    } else {
      // might be a csv
      return {
        'name': 'Parse data: ' + i,
        'task': parseCsvData,
        'params': [d.name, d.data]
      };
    }
  }));
};

var parseCsvData = function (tableName, data) {
  return new datawrap.Bluebird(function (fulfill, reject) {
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
        reject(e);
      }
    });
  });
};

var createSqlDatabase = function (tableName, columns, data, database) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var createTable = 'CREATE TABLE "' + tableName + '" (' + columns.map(function (column) {
      return '"' + column.name + '" ' + column.type.toUpperCase();
    }).join(', ') + ');';
    var insertStatement = 'INSERT INTO "' + tableName + '" (' + columns.map(function (column) {
      return '"' + column.name + '"';
    }).join(', ') + ') VALUES (' + columns.map(function (column) {
      return '{{' + column.name + '}}';
    }).join(', ') + ');';

    var taskList = [{
      'name': 'Create Database',
      'task': database.runQuery,
      'params': [createTable]
    }, {
      'name': 'Insert Data',
      'task': database.runQuery,
      'params': [insertStatement, data.map(function (row) {
        return tools.addTitles(columns.map(function (d) {
          return d.name;
        }), row);
      }), {
        'paramList': true
      }]
    }];

    datawrap.runList(taskList, 'Main Task')
      .then(function (a) {
        fulfill(a);
      }).catch(function (e) {
        reject(e);
      });
  });
};

var readCsvFile = function (name, filePath, fileOptions) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    fs.readFileAsync(filePath, fileOptions)
      .then(function (d) {
        parseCsvData(name, d)
          .then(fulfill)
          .catch(reject);
      })
      .catch(SyntaxError, reject);
  });
};
