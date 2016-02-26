// TODO take sources as inputs
// TODO make a connection for sources to be stored
// TODO add / remove sources from that connection

var CreateSource = require('./src/CreateSource');
var Promise = require('bluebird');
var fandlebars = require('fandlebars');

var cacheConfig = require('./sources/cache.json');

var sourceAConfig = require('./test/sources/sourceA.json');
var sourceBConfig = require('./test/sources/sourceB.json');

var SyncSources = function (cacheConfig) {
  return new Promise(function (fulfill, reject) {
    CreateSource()(cacheConfig).then(function (cache) {
      var createSource = new CreateSource(cache);
      fulfill(function () {
        return new Promise(function (fulfill, reject) {
          // Put the arguments into an array
          var sourceConfigs = [];
          for (var i in arguments) {
            sourceConfigs.push(arguments[i]);
          }

          // Load the sources for each of the sourceConfigs passed in
          Promise.all(sourceConfigs.map(function (sourceConfig) {
            return createSource(fandlebars.obj(sourceConfig, process));
          })).then(function (sources) {
            fulfill(sources);
          // return sync(sourceAConfig, sourceBConfig);
          }).catch(reject);
        });
      });
    }).catch(reject);
  });
};

SyncSources(cacheConfig).then(function (syncSources) {
  syncSources(sourceAConfig, sourceBConfig).then(console.log).catch(function (e) {
    console.log(e);
    throw (e);
  });
}).catch(function (e) {
  console.log(e);
  throw (e);
});
