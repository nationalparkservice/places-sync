var datawrap = require('datawrap');
var fs = datawrap.Bluebird.promisifyAll(require('fs'));
var csv = require('csv');

var arrayify = function (value) {
  return Array.isArray(value) ? value : [value];
};

var go = module.exports = function (config, defaults, options) {
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
      console.log('data');
      console.log(JSON.stringify(d, null, 2));
      createSqlDatabase(d[0].name, d[0].columns, d[0].data, db);
    }).catch(function (e) {
      console.log('Error');
      console.log(e);
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
            types[index] = getType(column, types[index]);
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
      return addTitles(columns.map(function(d){return d.name;}), row);
    }), {
      'paramList': true
    }]
  }, {
    'name': 'Verify Data',
    'task': database.runQuery,
    'params': ['SELECT * FROM "' + tableName + '";']
  }];

  datawrap.runList(taskList, 'Main Task')
    .then(function (a) {
      console.error(readOutput(a));
      console.log('success');
    }).catch(function (e) {
      console.error(readOutput(e));
      console.log('failure');
      console.log(e[e.length-1]);
      throw e[e.length - 1];
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

var getType = function (value, maxType) {
  var type = 'text';
  value = value.toString();
  if (!(isNaN(value) || value.replace(/ /g, '').length < 1) && maxType !== 'text') {
    type = 'float';
    if (parseFloat(value, 10) === parseInt(value, 10) && maxType !== 'float') {
      type = 'integer';
    }
  }
  return type;
};

var datawrapDefaults = require('../defaults');
var defaults = datawrap.fandlebars.obj(datawrapDefaults, global.process);
var addTitles = function (titles, data) {
  var returnValue = {};
  titles.forEach(function (title, index) {
    returnValue[title] = data[index];
  });
  return returnValue;
};

var readOutput = function (output) {
  return JSON.stringify(arrayify(output).map(function (o) {
    if (o.toString().substr(0, 5) === 'Error') {
      return o.toString + '\\n' + o.stack;
    } else {
      return JSON.stringify(o, null, 2);
    }
  }), null, 2).replace(/\\n/g, '\n');
};

go({
  'data': {
    'test': 'file:///test.csv'
  },
  'name': 'csvTest'
}, defaults);

