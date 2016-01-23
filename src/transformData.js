var tools = require('./tools');
var formats = tools.requireDirectory(__dirname + '/formats');
var guessFormat = require('./guessFormat');

module.exports = function (source, data) {
  source.data = data.data || data.toString();
  var dataFormat = source.format || guessFormat(source.data);
  if (formats[dataFormat]) {
    return formats[dataFormat](source);
  } else {
    return (tools.syncPromise(function () {
      return new Error('data format: ' + dataFormat + ' is not supported');
    }, true)());
  }
};
