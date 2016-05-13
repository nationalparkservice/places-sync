var Promise = require('bluebird');
var tools = require('../tools');
var sources = require('../sources');
Promise.config({
  longStackTraces: true
});

var processName = 'cartodb_poi_test';

var postgresConfig = {
  'name': 'rendered_poi',
  'connection': {
    'type': 'postgresql',
    'processName': processName,
    'host': '10.147.153.193',
    'user': 'postgres',
    'password': 'postgres',
    'database': 'test_places_pgs',
    'table': 'nps_cartodb_point_view'
  },
  'fields': {
    'primaryKey': 'cartodb_id',
    'lastUpdated': 'places_updated_at',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined
  },
  'filter': {
    'unit_code': ['cure', 'blca', 'CURE', 'BLCA']
  }
};
var cartodbConfig = {
  'name': 'places_test_points_cdb',
  'connection': {
    'type': 'cartodb',
    'processName': processName,
    'account': 'nps',
    'apiKey': 'http://docs.cartodb.com/cartodb-editor/your-account/#api-key',
    'table': 'points_of_interest_test'
  },
  'fields': {
    'primaryKey': 'key',
    'lastUpdated': 'last_update',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined,
    'mapped': {
      'the_geom_webmercator': null
    }
  },
  'filter': {
    'unit_code': ['cure', 'blca', 'CURE', 'BLCA']
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
},
  /* {
    'name': 'UpdatesFromCartoDB',
    'description': 'Gets an object containing the records that were updated and the records that were removed from sourceB since the last write to A in the masterCache',
    'task': '{{cartodbConnection.get.updates}}',
    'params': [postgresConfig.name],
    'operator': 'jstype',
    'expected': 'object'
  }*/
  {
    'name': 'applyUpdatesToCartoDB',
    'description': 'Adds the updates and removes to the b object',
    'task': '{{cartodbConnection.modify.applyUpdates}}',
    'params': ['{{updatesFromPostgresql}}'],
    'operator': 'jstype',
    'expected': 'array'
  },
  /* {
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
    },*/
  {
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
