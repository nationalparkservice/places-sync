var fs = require('fs');
var path = require('path');

module.exports = function (directory, ignoreList) {
  var regexp = new RegExp('(.+?)\.js(on)?$');
  var returnValue = {};
  fs.readdirSync(directory).forEach(function (file) {
    var match = file.match(regexp);
    if (match && ignoreList.indexOf(file) === -1) {
      returnValue[match[1]] = require(path.resolve(path.join(directory, file)));
    }
  });
  return returnValue;
};
