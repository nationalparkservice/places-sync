var Bluebird = require('datawrap').Bluebird;
var request = Bluebird.promisifyAll(require('request'));

module.exports = function (source) {
  return new Bluebird(function (fulfill, reject) {
    request.getAsync(source.data).then(function (response) {
      if (response.statusCode === 200) {
        fulfill(response.body);
      } else {
        reject(new Error('Invalid response code: ' + response.statusCode));
      }
    }).catch(function (e) {
      reject(e);
    });
  });
};
