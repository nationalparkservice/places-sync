var fs = require('datawrap').Bluebird.promisifyAll(require('fs'));

module.exports = function (source, regexps, dataDirectory) {
  if (source.extractionType === 'file') {
    source.data = source.data.replace(new RegExp(regexps[source.extractionType]), dataDirectory);
  }
  source.fileOptions = source.fileOptions || 'utf8';
  return fs.readFileAsync(source.data, source.fileOptions);
};
