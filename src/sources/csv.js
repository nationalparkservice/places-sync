var Bluebird = require('datawrap').Bluebird;
var csvDb = require('../csvDb');
var guid = require('../guid');
var sourceTemplate = require('./sqliteTemplate');

module.exports = function (file, columns, customFunctions) {
  return new Bluebird(function (fulfill, reject) {
    // Create Datasource for this csv file
    var tableName = guid();
    var config = {
      'name': tableName,
      'data': {}
    };
    var defaults = {
      'fileDesignator': 'file:///',
      'fileOptions': {
        'encoding': 'utf8'
      },
      'dataDirectory': __dirname,
      'rootDirectory': __dirname
    };

    config.data[tableName] = file;
    csvDb(config, defaults)
      .then(function (csvObj) {
        var db = csvObj.database;
        fulfill(sourceTemplate(tableName, columns, db));
      }).catch(reject);
  });
};
