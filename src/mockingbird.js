var Promise = require('bluebird');

// This is a library that wraps a promise to make it act like a callback
module.exports = function (callback) {
  return (callback && typeof callback === 'function') ? function (f) {
    f(function (res) {
      callback(null, res);
    },
      function (err) {
        callback(err);
      }
    );
  } : Promise;
};
