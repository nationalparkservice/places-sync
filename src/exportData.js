var tools = require('./tools');
var exporters = tools.requireDirectory(__dirname + '/exporters');
var datawrap = require('datawrap');
var Bluebird = datawrap.bluebird;

var getData = function (data) {
  var origin = {
    'source': data.source || data._source || {},
    'where': data.where
  };
  if (typeof origin.source._source === 'function') {
    return origin.source.getDataWhere(origin.where);
  } else {
    // Hopefully the data was passed in in the right format!
    return tools.syncPromise(function () {
      return data;
    });
  }
};

var runAction = function (action, destination, defaults, data) {
  return new Bluebird(function (fulfill, reject) {
    getData(data).then(function (rawData) {
      exporters[destination.format](destination, defaults)[action](rawData).then(fulfill).catch(reject);
    }).catch(reject);
  });
};

module.exports = function (destination, defaults) {
  if (exporters[destination && destination.format]) {
    var runner = function (action, data) {
      return runAction(action, destination, defaults, data);
    };
    return {
      'addData': function (data) {
        return runner('addData', data);
      },
      'removeData': function (data) {
        return runner('removeData', data);
      }
    };
  } else {
    throw new Error('Exporter data format: ' + destination && destination.format + ' is not supported');
  }
};
