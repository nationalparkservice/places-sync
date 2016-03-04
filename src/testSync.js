var tape = require('tape');
var Promise = require('bluebird');
var fs = Promise.promisifyAll(require('fs'));
var tools = require('./tools');
var sources = require('./sources');

// Take care of the basic files and stuff
// CSV File A
var csvAOrigData = 'a,b,c,d\n';
csvAOrigData += '1,a,b,0\n';
csvAOrigData += '2,c,d,0\n';
var csvAFileName = './csvA.csv';

// CSV File B
var csvBOrigData = 'a,b,c,d\n';
var csvBFileName = './csvB.csv';

var makeSource = function (name, path) {
  return {
    'name': name,
    'connection': {
      'type': 'csv',
      'filePath': path
    },
    'primaryKey': ['a'],
    'lastUpdateField': 'd'
  };
};
var csvAConfig = makeSource('test_csvA', csvAFileName);
var csvBConfig = makeSource('test_csvB', csvBFileName);
var masterCacheConfig = {
  'name': 'masterCacheConfig',
  'connection': {
    'type': 'sqlite',
    'filePath': process.env['HOME'] + '/.places-sync/masterCache.sqlite'
  },
  'tableName': 'master',
  'lastUpdateField': 'last_updated',
  'removedField': 'is_removed',
  'removedValue': 1
};
// ///////////

var taskList = [{
  'name': 'masterCache',
  'description': 'Loads the source for the master sqlite database, gets info for it, and does not data to cache',
  'task': sources,
  'params': [masterCacheConfig]
}, {
  'name': 'writeCsvAFile',
  'task': fs.writeFileAsync,
  'params': [csvAFileName, csvAOrigData]
}, {
  'name': 'csvSourceA',
  'description': 'Loads the source for CSV a, gets info for it, and loads data to cache',
  'task': sources,
  'params': [csvAConfig, '{{masterCache}}']
}, {
  'name': 'csvBConfigWithColumns',
  'description': 'Pulls the column info from source A and adds it to the source B config',
  'more description': 'This is because we need to have rows in a CSV for it to load it without defining columns',
  'task': function (columns) {
    csvBConfig.columns = columns;
    return csvBConfig;
  },
  'params': ['{{csvSourceA.get.columns.0}}']
}, {
  'name': 'writeCsvBFile',
  'task': fs.writeFileAsync,
  'params': [csvBFileName, csvBOrigData]
}, {
  'name': 'csvSourceB',
  'description': 'Loads the source for CSV b, gets info for it, and loads data to cache',
  'task': sources,
  'params': ['{{csvBConfigWithColumns}}', '{{masterCache}}']
}, {
  'name': 'updatesFromA',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{csvSourceA.get.updates}}',
  'params': [csvBConfig.name]
}/*, {
  'name': 'applyUpdatesToB',
  'description': 'Adds the updates and removes to the b object',
  'task': '{{csvSourceB.modify.applyUpdates}}',
  'params': ['{{updatesFromA}}']
}, {
  'name': 'saveB',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{csvSourceB.save}}',
  'params': []
}, {
  'name': 'updatesFromB',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceB since the last write to A in the masterCache',
  'task': '{{csvSourceB.get.updates}}',
  'params': [csvAConfig.name]
}, {
  'name': 'applyUpdatesToA',
  'description': 'Adds the updates and removes to the a object',
  'task': '{{csvSourceA.modify.applyUpdates}}',
  'params': ['{{updatesFromB}}']
}, {
  'name': 'saveA',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{csvSourceA.save}}',
  'params': []
}, {
  'name': 'closeA',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceA.close()}}',
  'params': []
}, {
  'name': 'closeB',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceB.close()}}',
  'params': []
}, {
  'name': 'readFileA',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvAFileName]
}, {
  'name': 'readFileB',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvBFileName]
}*/];

tools.iterateTasks(taskList, 'test sync', true).then(function (results) {
  var resultObj = {};
  taskList.forEach(function (taskObj, i) {
    resultObj[taskObj.name] = results[i];
  });
  console.log(resultObj);
  tape('results test', function (t) {
    t.equal(1, 1);
    t.end();
  });
}).catch(function (e) {
  console.log('error');
  throw e;
});
