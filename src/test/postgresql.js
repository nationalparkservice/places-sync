var Promise = require('bluebird');
var tools = require('../tools');
var sources = require('../sources');
Promise.config({
  longStackTraces: true
});

var processName = 'test_postgres';

var postgresConfig = {
  'name': 'sync_test',
  'connection': {
    'type': 'postgresql',
    'processName': processName,
    'host': 'localhost',
    'user': 'postgres',
    'password': 'postgres',
    'database': 'places_boundaries',
    'table': 'sync_test'
  },
  'fields': {
    'primaryKey': 'key',
    'lastUpdated': 'last_update',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined
  }
};
var postgresCsvConfig = {
  'name': 'postgresCsv',
  'connection': {
    'type': 'csv',
    'processName': processName,
    'filePath': './postgresCsv.csv'
  },
  'fields': {
    'primaryKey': 'key',
    'lastUpdated': 'last_update',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined
  }
};

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
  'name': 'postgresConnection',
  'description': 'Loads the source for postgres table a, gets info for it, and loads data to cache',
  'task': sources,
  'params': [postgresConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'csvConnectionWithColumns',
  'description': 'Pulls the column info from postgresql and adds it to the csv config',
  'more description': 'This is because we need to have rows in a CSV for it to load it without defining columns',
  'task': function (columns) {
    postgresCsvConfig.columns = columns;
    return postgresCsvConfig;
  },
  'params': ['{{postgresConnection.get.columns.0}}'],
  'operator': 'structureEqual',
  'expected': postgresCsvConfig
}, {
  'name': 'csvConnection',
  'description': 'Loads the source for CSV a, gets info for it, and loads data to cache',
  'task': sources,
  'params': ['{{csvConnectionWithColumns}}', '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'updatesFromPostgresql',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{postgresConnection.get.updates}}',
  'params': ['{{csvConnection.name}}'],
  'operator': 'jstype',
  'expected': 'object'
}/*, {
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
  'operator': 'jstype',
  'expected': 'array'
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
  'operator': 'jstype',
  'expected': 'array'
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
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveA_2',
  'description': 'Write data out to A and write it to the masterCache',
  'task': '{{csvSourceA_2.save}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
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
  'operator': 'jstype',
  'expected': 'array'
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
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveA_3',
  'description': 'Write data out to A and write it to the masterCache',
  'task': '{{csvSourceA_3.save}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
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
  'operator': 'jstype',
  'expected': 'array'
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
}*/
];

tools.iterateTapeTasks(taskList.slice(0, taskList.length - 0), true, true, true).then(function (results) {
  console.log('sync done');
}).catch(function (e) {
  if (e === undefined) {
    e = new Error('undefined error');
  }
  console.log('sync error');
  throw e;
});
