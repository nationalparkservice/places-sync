var Promise = require('bluebird');

module.exports = function (fulfillMsg, rejectMsg) {
  return new Promise(function (fulfill, reject) {
    if (rejectMsg) {
      reject(rejectMsg);
    } else {
      fulfill(fulfillMsg);
    }
  });
};
