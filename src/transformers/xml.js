var jsonSource = require('./json');
var Bluebird = require('datawrap').Bluebird;
var xmljs = require('xmljs_trans_js');

module.exports = function (source) {
  return new Bluebird(function (fulfill, reject) {
    // Add the data to the object
    if (source.root) {
      var roots = source.root.split('.');
      source.data = xmljs.jsonify(source.data);
      for (var i = 0; i < roots.length; i++) {
        source.data = source.data && source.data[roots[i]] ? source.data[roots[i]] : null;
      }
      if (source.data) {
        jsonSource(source).then(fulfill).catch(reject);
      } else {
        reject(new Error('XML Source Root ' + source.root + ' not found'));
      }
    } else {
      reject(new Error('XML Source Root not defined'));
    }
  });
};
