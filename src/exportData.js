var tools = require('./tools');
var exporters = tools.requireDirectory(__dirname + '/exporters');

module.exports = function (destination, defaults) {
  if (exporters[destination && destination.format]) {
    return exporters[destination.format](destination, defaults);
  } else {
    throw new Error('Exporter data format: ' + destination && destination.format + ' is not supported');
  }
};
