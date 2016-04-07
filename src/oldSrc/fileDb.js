var datawrap = require('datawrap');
var fs = datawrap.Bluebird.promisifyAll(require('fs'));
var createTable = require('./createTable');
var csvToTable = require('./csvToTable');
var jsonToTable = require('./jsonToTable');
var geojsonToTable = require('./geojsonToJsonTable');
var request = require('request');

module.exports = function (config, defaults, options) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var db = datawrap({
      'type': 'sqlite',
      'name': config.name,
      'connection': ':memory:'
    }, defaults);
    options = options || {};

    var regex = {
      'file': new RegExp('^' + (options.fileDesignator || config.fileDesignator || defaults.fileDesignator)),
      'url': new RegExp('^' + 'https?://')
    };
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
      'data': inData[record]
    });
  }
  return datawrap.runList(arrayInData.map(function (d, i) {
    var data = typeof d.data === 'string' ? d.data : d.data.path || d.data.data;
    if (data.match(regex.file)) {
      // is a file
      return {
        'name': 'Read file: ' + d.name,
        'task': readFile,
        'params': [d.name, data.replace(regex.file, dataDirectory), d.data, fileOptions]
      };
    } else if (data.match(regex.url)) {
      return {
        'name': 'Read From URL: ' + d.name,
        'task': readUrl,
        'params': [d.name, data.replace(regex.url, dataDirectory), d.data]
      };
    } else {
      return {
        'name': 'Parse data: ' + i,
        'task': parseData,
        'params': [d.name, data, d.data]
      };
    }
  }));
};

var parseData = function (tableName, data, dataOptions) {
  var dataFormat = dataOptions.format || guessFormat(data);
  var formats = {
    'csv': csvToTable,
    'json': jsonToTable,
    'geojson': function (name, d) {
      return jsonToTable(name, geojsonToTable(d));
    }
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

var readUrl = function (name, filePath, dataOptions) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    request(filePath, function (error, response, body) {
      if (!error && response.statusCode === 200) {
        fulfill(body);
      } else {
        reject(error || new Error(filePath + ' returned status code: ' + response.statusCode));
      }
    });
  });
};

var readFile = function (name, filePath, dataOptions, fileOptions) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    fs.readFileAsync(filePath, fileOptions)
      .then(function (d) {
        parseData(name, d, dataOptions)
          .then(fulfill)
          .catch(reject);
      })
      .catch(SyntaxError, reject);
  });
};

var guessFormat = function (data) {
  var dataFormat;
  var tmp;
  if (typeof data === 'object') {
    dataFormat = 'json';
    if (data.type && data.type === 'FeatureCollection') {
      data = 'geojson';
    }
  } else if (typeof data === 'string') {
    try {
      tmp = JSON.parse(data);
      if (tmp.type && tmp.type === 'FeatureCollection') {
        dataFormat = 'geojson';
      } else {
        dataFormat = 'json';
      }
    } catch (error) {
      dataFormat = 'csv';
    }
  }
  return dataFormat;
};
