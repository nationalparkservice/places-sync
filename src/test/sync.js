var Promise = require('bluebird');
var fs = Promise.promisifyAll(require('fs'));
var tools = require('../tools');
var sources = require('../sources');
Promise.config({
  longStackTraces: true
});

// Take care of the basic files and stuff
// CSV File A
var csvAOrigData = 'a,b,c,d\n';
csvAOrigData += '1a' + ',a,b,' + new Date().getTime() + '\n';
csvAOrigData += '2a' + ',c,d,' + new Date().getTime() + '\n';
var csvAFileName = './csvA.csv';

// CSV File B
var csvBOrigData = 'a,b,c,d\n';
csvAOrigData += '3b' + ',e,f,' + (new Date().getTime() + 1) + '\n';
var csvBFileName = './csvB.csv';


var processName = tools.guid();

var makeSource = function (name, path) {
  return {
    'name': name,
    'connection': {
      'type': 'csv',
      'processName': processName,
      'filePath': path
    },
    'fields': {
      'primaryKey': 'a',
      'lastUpdated': 'd',
      'removed': undefined,
      'removedValue': undefined,
      'forced': undefined
    }
  };
};

var csvAConfig = makeSource('test_csvA', csvAFileName);
var csvBConfig = makeSource('test_csvB', csvBFileName);

var masterCacheConfig = {
  'name': 'masterCacheConfig',
  'connection': {
    'type': 'sqlite',
    'table': 'master',
    'filePath': process.env['HOME'] + '/.places-sync/masterCache.sqlite'
  },
  'fields': {
    // 'primaryKey': ['key', 'process', 'source'], // These do need to be defined for SQLite, because we can get this info from SQLite
    'lastUpdated': 'last_updated',
    'hash': 'hash',
    'removed': 'is_removed',
    'removedValue': 1,
    'forced': undefined
  }
};

var sourceObjStructure = {
  cache: {
    selectAll: {}
  },
  close: {},
  get: {
    all: {},
    allKeys: {},
    columns: {},
    lastUpdate: {},
    updates: {}
  },
  modify: {
    applyUpdates: {},
    create: {},
    remove: {},
    update: {}
  },
  save: {}
};
// ///////////

var taskList = [{
  'name': 'masterCache',
  'description': 'Loads the source for the master sqlite database, gets info for it, and does not load data to cache',
  'task': sources,
  'params': [masterCacheConfig],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'writeCsvAFile',
  'desciption': 'Writes the csv A file out',
  'task': fs.writeFileAsync,
  'params': [csvAFileName, csvAOrigData],
  'operator': 'equal',
  'expected': undefined
}, {
  'name': 'csvSourceA',
  'description': 'Loads the source for CSV a, gets info for it, and loads data to cache',
  'task': sources,
  'params': [csvAConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'csvBConfigWithColumns',
  'description': 'Pulls the column info from source A and adds it to the source B config',
  'more description': 'This is because we need to have rows in a CSV for it to load it without defining columns',
  'task': function (columns) {
    csvBConfig.columns = columns;
    return csvBConfig;
  },
  'params': ['{{csvSourceA.get.columns.0}}'],
  'operator': 'structureEqual',
  'expected': makeSource('test')
}, {
  'name': 'writeCsvBFile',
  'desciption': 'Writes the csv B file to disk',
  'task': fs.writeFileAsync,
  'params': [csvBFileName, csvBOrigData],
  'operator': 'equal',
  'expected': undefined

}, {
  'name': 'csvSourceB',
  'description': 'Loads the source for CSV b, gets info for it, and loads data to cache',
  'task': sources,
  'params': ['{{csvBConfigWithColumns}}', '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'updatesFromA',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{csvSourceA.get.updates}}',
  'params': [csvBConfig.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToB',
  'description': 'Adds the updates and removes to the b object',
  'task': '{{csvSourceB.modify.applyUpdates}}',
  'params': ['{{updatesFromA}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveB',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{csvSourceB.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'updatesFromB',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceB since the last write to A in the masterCache',
  'task': '{{csvSourceB.get.updates}}',
  'params': [csvAConfig.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToA',
  'description': 'Adds the updates and removes to the a object',
  'task': '{{csvSourceA.modify.applyUpdates}}',
  'params': ['{{updatesFromB}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveA',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{csvSourceA.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'closeA',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceA.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}, {
  'name': 'closeB',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceB.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}, {
  'name': 'readFileA',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvAFileName, 'utf8'],
  'operator': 'jstype',
  'expected': 'string'

}, {
  'name': 'readFileB',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvBFileName, 'utf8'],
  'operator': 'deepEqual',
  'expected': '{{readFileA}}'
}, {
  'name': 'csvSourceA_2',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [csvAConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'csvSourceB_2',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [csvBConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'csvAAddRow',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': '{{csvSourceA_2.modify.create}}',
  'params': [{
    'a': 'testValue',
    'b': '10',
    'c': '12',
    'd': new Date().getTime() + 5
  }],
  'operator': 'structureEqual',
  'expected': {'update': {}}
}, {
  'name': 'saveA_2',
  'description': 'Write data out to A and write it to the masterCache',
  'task': '{{csvSourceA_2.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'updatesFromA_2',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{csvSourceA_2.get.updates}}',
  'params': [csvBConfig.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToB_2',
  'description': 'Adds the updates and removes to the b object',
  'task': '{{csvSourceB_2.modify.applyUpdates}}',
  'params': ['{{updatesFromA_2}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveB_2',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{csvSourceB_2.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'closeA_2',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceA_2.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}, {
  'name': 'closeB_2',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceB_2.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}, {
  'name': 'readFileA_2',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvAFileName, 'utf8'],
  'operator': 'jstype',
  'expected': 'string'

}, {
  'name': 'readFileB_2',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvBFileName, 'utf8'],
  'operator': 'deepEqual',
  'expected': '{{readFileA_2}}'
}, {
  'name': 'csvSourceA_3',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [csvAConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'csvSourceB_3',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [csvBConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'csvARemoveRow',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': '{{csvSourceA_3.modify.remove}}',
  'params': [{
    'a': 'testValue'
  }],
  'operator': 'structureEqual',
  'expected': {'remove': []}
}, {
  'name': 'saveA_3',
  'description': 'Write data out to A and write it to the masterCache',
  'task': '{{csvSourceA_3.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'updatesFromA_3',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{csvSourceA_3.get.updates}}',
  'params': [csvBConfig.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToB_3',
  'description': 'Adds the updates and removes to the b object',
  'task': '{{csvSourceB_3.modify.applyUpdates}}',
  'params': ['{{updatesFromA_3}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveB_3',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{csvSourceB_3.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'closeA_3',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceA_3.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}, {
  'name': 'closeB_3',
  'description': 'Closes the Source and frees up memory',
  'task': '{{csvSourceB_3.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}, {
  'name': 'readFileA_3',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvAFileName, 'utf8'],
  'operator': 'jstype',
  'expected': 'string'

}, {
  'name': 'readFileB_3',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [csvBFileName, 'utf8'],
  'operator': 'deepEqual',
  'expected': '{{readFileA_3}}'
}];

tools.iterateTapeTasks(taskList.slice(0, taskList.length), true, true, true).then(function (results) {
  console.log('sync done');
}).catch(function (e) {
  if (e === undefined) {
    e = new Error('undefined error');
  }
  console.log('sync error');
  throw e;
});
