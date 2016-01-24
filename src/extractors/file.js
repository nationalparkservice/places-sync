var fs = require('datawrap').Bluebird.promisifyAll(require('fs'));

module.exports = function (source) {
  source.fileOptions = source.fileOptions || 'utf8';
  return fs.readFileAsync(source.data, source.fileOptions);
};
