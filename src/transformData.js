var tools = require('./tools');
var formats = tools.requireDirectory(__dirname + '/formats');
var guessFormat = require('./guessFormat');

module.exports = function (source, data) {
  if (typeof data === 'object') {
    // It might have some values we want in our source
    for (var field in data) {
      console.log('has field', field);
      source[field] = data[field];
    }
  } else {
    source.data = data.toString();
  }
  var dataFormat = source.format || guessFormat(source.data);
  if (formats[dataFormat]) {
    return formats[dataFormat](source);
  } else {
    return (tools.syncPromise(function () {
      return new Error('data format: ' + dataFormat + ' is not supported');
    }, true)());
  }
};
