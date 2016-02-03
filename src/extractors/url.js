var Bluebird = require('datawrap').Bluebird;
var superagent = Bluebird.promisifyAll(require('superagent'));

module.exports = function (source) {
  return new Bluebird(function (fulfill, reject) {
    superagent.getAsync(source.data).then(function (response) {
      if (response.statusCode === 200) {
        fulfill(response.text);
      } else {
        reject(new Error('Invalid response code: ' + response.statusCode));
      }
    }).catch(function (e) {
      reject(e);
    });
  });
};
