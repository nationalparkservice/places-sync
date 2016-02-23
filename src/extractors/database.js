var datawrap = require('datawrap');
var mainDefaults = require('../../defaults');

module.exports = function (source) {
  // Source will include "data", which is the SQL query in this case, it can also be a file starting with "file:///"
  // Source can include query "params" parameters in an object (using handlebars-like syntax)
  // Source will also include "connection" which looks like this
  //   {'type': 'cartodb', 'account': 'ACCOUNTNAME', 'apiKey': 'API KEY HERE'}
  //   {'type': 'postgresql', 'port': 5432, 'username': 'USERNAME', 'password': 'PASSWORD', 'address': 'ADDRESS'}
  return new datawrap.Bluebird(function (fulfill, reject) {
    datawrap(source.connection, source.defaults || mainDefaults).runQuery(source.data, source.params, {}).then(function (result) {
      // Clean up result for other tools
      var finalResult = result[result.length - 1];
      var returnValue = {
        data: finalResult.rows//,
        // predefinedColumns: undefined // TODO: Fill this in!
      };
      for (var newField in returnValue) {
        source[newField] = returnValue[newField];
      }
      fulfill(source);
    }).catch(function (e) {
      reject(e);
    });
  });
};
