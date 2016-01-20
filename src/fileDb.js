var datawrap = require('datawrap');
var fs = datawrap.Bluebird.promisifyAll(require('fs'));
var createTable = require('./createTable');
var csvToTable = require('./csvToTable');
var jsonToTable = require('./jsonToTable');

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
    var dataDirectory = (options.dataDirectory || config.dataDirectory || defaults.dataDirectory);
    dataDirectory = dataDirectory + (dataDirectory.substr(-1) === '/' ? '' : '/');
    var inData = config.data;

    getData(inData, dataDirectory, fileOptions, regex)
      .then(function (d) {
        // Go through the data and add it to the database
        var taskList = d.map(function (table) {
          return {
            'name': 'Create ' + table.name,
            'task': createTable,
            'params': [table.name, table.columns, table.data, db]
          };
        });
        datawrap.runList(taskList)
          .then(function (r) {
            fulfill({
              result: r,
              database: db
            });
          }).catch(function (e) {
          reject(e);
        });
      }).catch(function (e) {
      reject(e);
    });
  });
};

var getData = function (inData, dataDirectory, fileOptions, regex) {
  var arrayInData = [];
  for (var record in inData) {
    arrayInData.push({
      'name': record,
      'data': inData[record],
      'format': fileOptions.format
    });
  }
  return datawrap.runList(arrayInData.map(function (d, i) {
    if (d.data.match(regex)) {
      // is a file
      return {
        'name': 'Read file: ' + d.name,
        'task': readFile,
        'params': [d.name, d.data.replace(regex, dataDirectory), fileOptions]
      };
    } else {
      return {
        'name': 'Parse data: ' + i,
        'task': parseData,
        'params': [d.name, d.data, d.format]
      };
    }
  }));
};

var parseData = function (tableName, data, dataFormat) {
  dataFormat = dataFormat || guessFormat(data);
  var formats = {
    'csv': csvToTable,
    'json': jsonToTable
  };

  if (formats[dataFormat]) {
    return formats[dataFormat](tableName, data);
  } else {
    return new datawrap.Bluebird(function (fulfill, reject) {
      var e = new Error('data format: ' + dataFormat + ' is not supported');
      reject(e);
    });
  }
};


var readFile = function (name, filePath, fileOptions) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    fs.readFileAsync(filePath, fileOptions)
      .then(function (d) {
        parseData(name, d, fileOptions.format)
          .then(fulfill)
          .catch(reject);
      })
      .catch(SyntaxError, reject);
  });
};

var guessFormat = function (data) {
  var dataFormat;
  if (typeof data === 'object') {
    dataFormat = 'json';
  } else if (typeof data === 'string') {
    try {
      JSON.parse(data);
      dataFormat = 'json';
    } catch (error) {
      dataFormat = 'csv';
    }
  }
  return dataFormat;
};
