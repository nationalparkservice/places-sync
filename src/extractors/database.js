var datawrap = require('datawrap');
var mainDefaults = require('../../defaults');

module.exports = function (source) {
  // Source will include "data", which is the SQL query in this case, it can also be a file starting with "file:///"
  // Source can include query "params" parameters in an object (using handlebars-like syntax)
  // Source will also include "connection" which looks like this
  //   {'type': 'cartodb', 'account': 'ACCOUNTNAME', 'apiKey': 'API KEY HERE'}
  //   {'type': 'postgresql', 'port': 5432, 'username': 'USERNAME', 'password': 'PASSWORD', 'address': 'ADDRESS'}
  return datawrap(source.connection, source.defaults || mainDefaults).runQuery(source.data, source.params, {});
};
