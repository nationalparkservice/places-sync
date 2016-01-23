var datawrap = require('datawrap');
var Mockingbird = datawrap.Mockingbird;
var tools = require('./tools');
var guid = require('./guid');
// var createTable = require('.createTable');
var mainDefaults = require('../defaults');

var extractors = tools.requireDirectory('./extractors');

var transformData = require('./transformData');
var loadData = require('./loadData');
var createSource = require('./createSource');

// Creates a database in memory to store incoming data
module.exports = function (config, options, defaults) {
  // Merge the configs together
  config = buildConfig(config, options, defaults);

  // Once we have the config, we can create the database
  var database = createDatabase(config);
  var sources = {};
  var sourcesLoaded = false;

  return {
    'addData': function (source, callback) {
      // Imports the data from an object containing
      // the following information: (name and data are the only required field)
      //    {name: (name for source) data: (see below), format: CSV, JSON, GEOJSON, columns: [{name: 'column', type: 'text'}], extractor: FILE, URL, CUSTOM}
      //
      // the data field can be either:
      //   1: A string that is CSV, JSON, or GEOJSON
      //   2: A string that uses a designator and a type to load a file or url or a custom source
      //     Examples: file:///test.csv, http://github.com
      return new (Mockingbird(callback))(function (fulfill, reject) {
        loadSource(source.name, source, config.regexps, config.dataDirectory, database).then(function (result) {
          fulfill('success'); // TODO return something better!
        }).catch(function (error) {
          reject(error); // TODO better errors
        });
      });
    },
    'load': function (callback) {
      return new (Mockingbird(callback))(function (fulfill, reject) {
        if (!sourcesLoaded) {
          loadSources(config.data, config.regexps, config.dataDirectory, database).then(function (result) {
            sourcesLoaded = true;
            console.log(result);
            fulfill('success'); // TODO return something better!
          }).catch(function (error) {
            reject(error); // TODO better errors
          });
        }
      });
    },
    'sources': sources,
    'database': database
  };
};

var buildConfig = function (config, options, defaults) {
  var newConfig = {
    regexps: {}
  };

  // Order of importance: options, config, defaults
  var inputs = [
    options || {},
    config || {},
    defaults || mainDefaults
  ];
  var i, field, designatorType;

  // Assign the values to the new config in order of importance
  for (i = 0; i < inputs.length; i++) {
    for (field in inputs[i]) {
      if (!newConfig[field]) {
        newConfig[field] = inputs[i][field];

        // If it looks like a Designator, let's create a regexp for it
        designatorType = field.match(/(.+?)Designator$/);
        if (designatorType && designatorType[1]) {
          newConfig.regexps[designatorType[1]] = new RegExp('^' + inputs[i][field]);
        }
      }
    }
  }

  return newConfig;
};

var createDatabase = function (config) {
  return datawrap({
    'type': 'sqlite',
    'name': config.name || guid(),
    'connection': config.connection || ':memory:'
  }, config);
};

var loadSources = function (sources, regexps, dataDirectory, database) {
  // Convert these inputs into a standard format
  var taskList = [];

  for (var name in sources) {
    taskList.push({
      'name': 'Load ' + name,
      'task': loadSource,
      'params': [name, sources[name], regexps, dataDirectory, database]
    });
  }

  return datawrap.runList(taskList);
};

var loadSource = function (name, source, regexps, dataDirectory, database) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    // If the data field is a file, url, or custom, we need to fill that in, otherwise we can skip to parsing it
    var type;
    var taskList = [];
    source.name = source.name || name;
    if (source.extractor) {
      for (type in regexps) {
        if (source.data.match(regexps[type])) {
          source.extractor = type;
        }
      }
    }
    if (source.extractor === 'file') {
      source.data = source.data.replace(regexps[type], dataDirectory);
    }

    taskList = [{
      // Add the extraction task
      'name': 'Extract ' + source.name,
      'task': extractors[source.extractionType || 'none'],
      'params': [source]
    }, {
      // Add the task to parse the data
      'name': 'Transform ' + source.name,
      'task': transformData,
      'params': ['{{Extract' + source + '}}']
    }, {
      // Add the task to import the data to sqlite
      'name': 'Load ' + source.name,
      'task': loadData,
      'params': ['{{Parse' + source + '}}']
    }, {
      // Add the task to import the data to sqlite
      'name': 'Create source ' + source.name,
      'task': createSource,
      'params': ['{{Parse' + source + '}}']
    }];

    datawrap.runList(taskList).then(function (r) {
      fulfill(r[r.length - 1]);
    }).catch(function (e) {
      reject(e[e.length - 1]);
    });
  });
};
