var datawrap = require('datawrap');
var tools = require('./tools');

// ETL Tools
var extractors = tools.requireDirectory(__dirname + '/extractors');
var transformData = require('./transformData');
var loadData = require('./loadData');
var createSource = require('./createSource');

module.exports = function (name, source, regexps, dataDirectory, database) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    // If the data field is a file, url, or custom, we need to fill that in, otherwise we can skip to parsing it
    var type;
    var taskList = [];
    source.name = source.name || name;
    if (!source.extractionType) {
      for (type in regexps) {
        if (source.data.match(new RegExp(regexps[type]))) {
          source.extractionType = type;
        }
      }
    }
    if (source.extractionType === 'file') {
      source.data = source.data.replace(new RegExp(regexps[source.extractionType]), dataDirectory);
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
      'params': [source, '{{Extract ' + source.name + '}}']
    }, {
      // Add the task to import the data to sqlite
      'name': 'Load ' + source.name,
      'task': loadData,
      'params': [source, '{{Transform ' + source.name + '}}', database]
    }, {
      // Add the task to import the data to sqlite
      'name': 'Create source ' + source.name,
      'task': createSource,
      'params': [source, database]
    }];

    datawrap.runList(taskList).then(function (r) {
      fulfill(r[r.length - 1]);
    }).catch(reject);
  });
};
