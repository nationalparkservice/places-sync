var datawrap = require('datawrap');
var guid = require('./guid');
var mainDefaults = require('../defaults');
var Mockingbird = datawrap.mockingbird;
var loadSource = require('./loadSource');
var tools = require('./tools');

// Creates a database in memory to store incoming data
module.exports = function (config, options, defaults) {
  // Merge the configs together
  config = buildConfig(config, options, defaults);

  // Once we have the config, we can create the database
  var database = getDatabase(config);
  var sources = {};
  var sourcesLoaded = false;

  return {
    'addData': function (source, callback) {
      // Imports the data from an object containing
      // the following information: (name and data are the only required field)
      //    {name: (name for source) data: (see below), format: CSV, JSON, GEOJSON, columns: [{name: 'column', type: 'text'}], extractionType: FILE, URL, ARCGIS}
      //
      // the data field can be either:
      //   1: A string that is CSV, JSON, or GEOJSON
      //   2: A string that uses a designator and a type to load a file or url or a custom source
      //     Examples: file:///test.csv, http://github.com
      return new (Mockingbird(callback))(function (fulfill, reject) {
        loadSource(source.name, source, config.regexps, config.dataDirectory, database).then(function (result) {
          sources[source.name] = result[0];
          fulfill(result[0]); // TODO return something better!
        }).catch(function (error) {
          console.log(tools.readOutput(error));
          if (Array.isArray(error)) {
            error = error[error.length - 1];
          }
          reject(error); // TODO better errors
        });
      });
    },
    'load': function (callback) {
      // Loads the databases soecified in the config
      return new (Mockingbird(callback))(function (fulfill, reject) {
        if (!sourcesLoaded) {
          loadSources(config.data, config.regexps, config.dataDirectory, database).then(function (result) {
            sourcesLoaded = true;
            result.forEach(function (source) {
              sources[source.name] = source;
            });
            fulfill(sources); // TODO return something better!
          }).catch(function (error) {
          console.log(tools.readOutput(error));
            reject(error); // TODO better errors
          });
        }
      });
    },
    'sources': sources,
    'database': database,
    'name': config.name
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
          newConfig.regexps[designatorType[1]] = '^' + inputs[i][field];
        }
      }
    }
  }

  newConfig.name = newConfig.name || guid();
  newConfig.dataDirectory = newConfig.dataDirectory + (newConfig.dataDirectory.substr(-1) === '/' ? '' : '/');
  newConfig = datawrap.fandlebars.obj(newConfig, global.process);
  return newConfig;
};

var getDatabase = function (config) {
  return datawrap({
    'type': 'sqlite',
    'name': config.name,
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
