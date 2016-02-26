var tools = require('../tools');
var sourceTypes = tools.requireDirectory('../sourceTypes');
var Bluebird = require('Bluebird');
var runList = tools.runList;

module.exports = function(source) {
  return new Bluebird(fulfill, reject) {
    var tasks = {
      'verifyConnection': {
        'description': 'Check that this type of connection is supported for the required permissions',
        'task': function(type, permissions) {
          var returnValue = false;
          if (sourceTypes[type]) {
            returnValue = true;
            for (var i = 0; i < permissions.length; i++) {
              if (sourceTypes[type].permissions.toLowerCase().indexOf(permissions.toLowerCase()[i]) === -1)
                returnValue = false;
            }
          }
          return returnValue;
        },
        'params': [source.connection.type, source.permissions]
      },
      'runInitialize': {
        'description': 'Each source type should have its own initializtion process, so once we know that what we want is supported, we use that',
        'task': sourceTypes[type].initialize,
        'params': source
    };
  }
};
