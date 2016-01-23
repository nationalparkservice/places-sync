var fs = require('datawrap').Bluebird.promisifyAll(require('fs'));

module.exports = function (source) {
  return fs.readFileAsync(source.data, source.fileOptions);
};
