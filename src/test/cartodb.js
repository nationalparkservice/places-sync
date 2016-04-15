var Promise = require('bluebird');
var tools = require('../tools');
var sources = require('../sources');
Promise.config({
  longStackTraces: true
});

var processName = 'test_cartodb';

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
var cartodbConfig = {
  'name': 'postgresCartoDB',
  'connection': {
    'type': 'cartodb',
    'processName': processName,
    'account': 'nps',
    'apiKey': 'http://docs.cartodb.com/cartodb-editor/your-account/#api-key',
    'tableName': 'sync_test'
  },
  'fields': {
    'primaryKey': 'key',
    'lastUpdated': 'last_update',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined,
    'mapped': {
      'cartodb_id': null,
      'the_geom': null,
      'the_geom_webmercator': null
    }
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
  'name': 'cartodbConnection',
  'description': 'Loads the source for CSV a, gets info for it, and loads data to cache',
  'task': sources,
  'params': [cartodbConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'updatesFromPostgresql',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{postgresConnection.get.updates}}',
  'params': ['{{cartodbConnection.get.name}}'],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'UpdatesFromCartoDB',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceB since the last write to A in the masterCache',
  'task': '{{cartodbConnection.get.updates}}',
  'params': [postgresConfig.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToCartoDB',
  'description': 'Adds the updates and removes to the b object',
  'task': '{{cartodbConnection.modify.applyUpdates}}',
  'params': ['{{updatesFromPostgresql}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'applyUpdatesToPostgres',
  'description': 'Adds the updates and removes to the postgres object',
  'task': '{{postgresConnection.modify.applyUpdates}}',
  'params': ['{{UpdatesFromCartoDB}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'savePostgres',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{postgresConnection.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'closePostgres',
  'description': 'Closes the Source and frees up memory',
  'task': '{{postgresConnection.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}, {
  'name': 'saveCartoDB',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{cartodbConnection.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}, {
  'name': 'closeCartoDB',
  'description': 'Closes the Source and frees up memory',
  'task': '{{cartodbConnection.close}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    _events: {},
    filename: ':memory:',
    mode: 65542,
    open: false
  }
}
/*, {
  'name': 'readFileA',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [cartodbAFileName, 'utf8'],
  'operator': 'jstype',
  'expected': 'string'

}, {
  'name': 'readFileB',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [cartodbBFileName, 'utf8'],
  'operator': 'deepEqual',
  'expected': '{{readFileA}}'
}, {
  'name': 'cartodbSourceA_2',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [postgresConnection, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'cartodbSourceB_2',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [cartodbConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'cartodbAAddRow',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': '{{cartodbSourceA_2.modify.create}}',
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
  'task': '{{cartodbSourceA_2.save}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'updatesFromA_2',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{cartodbSourceA_2.get.updates}}',
  'params': [cartodbConfig.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToB_2',
  'description': 'Adds the updates and removes to the b object',
  'task': '{{cartodbSourceB_2.modify.applyUpdates}}',
  'params': ['{{updatesFromA_2}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveB_2',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{cartodbSourceB_2.save}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'closeA_2',
  'description': 'Closes the Source and frees up memory',
  'task': '{{cartodbSourceA_2.close}}',
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
  'task': '{{cartodbSourceB_2.close}}',
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
  'params': [cartodbAFileName, 'utf8'],
  'operator': 'jstype',
  'expected': 'string'

}, {
  'name': 'readFileB_2',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [cartodbBFileName, 'utf8'],
  'operator': 'deepEqual',
  'expected': '{{readFileA_2}}'
}, {
  'name': 'cartodbSourceA_3',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [postgresConnection, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'cartodbSourceB_3',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': sources,
  'params': [cartodbConfig, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'cartodbARemoveRow',
  'description': 'Loads the source for CSV a again, gets info for it, and loads data to cache',
  'task': '{{cartodbSourceA_3.modify.remove}}',
  'params': [{
    'a': 'testValue'
  }],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveA_3',
  'description': 'Write data out to A and write it to the masterCache',
  'task': '{{cartodbSourceA_3.save}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'updatesFromA_3',
  'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to B in the masterCache',
  'task': '{{cartodbSourceA_3.get.updates}}',
  'params': [cartodbConfig.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToB_3',
  'description': 'Adds the updates and removes to the b object',
  'task': '{{cartodbSourceB_3.modify.applyUpdates}}',
  'params': ['{{updatesFromA_3}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveB_3',
  'description': 'Write data out to B and write it to the masterCache',
  'task': '{{cartodbSourceB_3.save}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'closeA_3',
  'description': 'Closes the Source and frees up memory',
  'task': '{{cartodbSourceA_3.close}}',
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
  'task': '{{cartodbSourceB_3.close}}',
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
  'params': [cartodbAFileName, 'utf8'],
  'operator': 'jstype',
  'expected': 'string'

}, {
  'name': 'readFileB_3',
  'description': 'Read the data to compare',
  'task': fs.readFileAsync,
  'params': [cartodbBFileName, 'utf8'],
  'operator': 'deepEqual',
  'expected': '{{readFileA_3}}'
}*/
];

if (cartodbConfig.connection.apiKey.substr(0, 4) === 'http') {
  var stdin = process.stdin;
  var stdout = process.stdout;
  stdin.resume();
  var question = "What's your CartoDB Key? (" + cartodbConfig.connection.apiKey + ')';
  stdout.write(question + ': ');
  stdin.once('data', function (data) {
    data = data.toString().trim();
    cartodbConfig.connection.apiKey = data;
    run(function () {
      process.exit(0);
    });
  });
} else {
  run();
}

var run = function (callback) {
  tools.iterateTapeTasks(taskList.slice(0, taskList.length - 0), true, true, true).then(function (results) {
    console.log('sync done');
    callback && callback();
  }).catch(function (e) {
    if (e === undefined) {
      e = new Error('undefined error');
    }
    console.log('sync error');
    throw e;
    callback && callback();
  });
};
