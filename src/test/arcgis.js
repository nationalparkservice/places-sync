var Promise = require('bluebird');
var tools = require('../tools');
var sources = require('../sources');
Promise.config({
  'longStackTraces': true
});

var arcgisSource = {
  'name': 'BLCACURE test',
  'connection': {
    'url': 'http://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/BLCACURE/FeatureServer/1',
    'placesDatatype': 'poi',
    'processName': 'places-sync DEV',
    'type': 'arcgis'
  },
  'fields': {
    'primaryKey': 'GEOMETRYID',
    'lastUpdated': 'EditDate'
  }
};

var jsonSource = {
  'name': 'jsonArcGis',
  'connection': {
    'type': 'json',
    'processName': 'places-sync DEV',
    'filePath': './jsonArcgis.json'
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
    'foreignKey': ['foreign_key'],
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

var taskList = [{
  'name': 'masterCache',
  'description': 'Loads the source for the master sqlite database, gets info for it, and does not load data to cache',
  'task': sources,
  'params': [masterCacheConfig],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'arcGisConnection',
  'task': sources,
  'params': [arcgisSource, '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'arcGisColumns',
  'description': 'Pulls the column info from source A and adds it to the source B config',
  'more description': 'This is because we need to have rows in a JSON for it to load it without defining columns',
  'task': function (columns) {
    return columns;
  },
  'params': ['{{arcGisConnection.get.columns.0}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'updatedJsonConfig',
  'description': 'Adds the columns info to the JSON config',
  'task': tools.setProperty,
  'params': ['columns', '{{arcGisColumns}}', jsonSource],
  'operator': 'structureEqual',
  'expected': jsonSource
}, {
  'name': 'jsonSource',
  'description': 'Loads the JSON source to sync with',
  'task': sources,
  'params': ['{{updatedJsonConfig}}', '{{masterCache}}'],
  'operator': 'structureEqual',
  'expected': sourceObjStructure
}, {
  'name': 'jsonColumns',
  'description': 'Just to make sure the columns are correct',
  'task': function (columns) {
    return columns;
  },
  'params': ['{{jsonSource.get.columns.0}}'],
  'operator': 'structureEqual',
  'expected': '{{arcGisColumns}}'
}, {
  'name': 'updatesFromArcGis',
  'description': 'Gets an object containing the records that were updated and the records that were removed from arcgis in the masterCache',
  'task': '{{arcGisConnection.get.updates}}',
  'params': [jsonSource.name],
  'operator': 'jstype',
  'expected': 'object'
}, {
  'name': 'applyUpdatesToJson',
  'description': 'Adds the changes to the JSON source',
  'task': '{{jsonSource.modify.applyUpdates}}',
  'params': ['{{updatesFromArcGis}}'],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'jsonSelectAll',
  'description': 'Check to see that the updates were made',
  'task': '{{jsonSource.cache.selectAll}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'saveJson',
  'description': 'Write data out to JSON file and tell the masterCache that we successfully copied the data',
  'task': '{{jsonSource.save}}',
  'params': [],
  'operator': 'structureEqual',
  'expected': {
    'updated': [],
    'removed': []
  }
}];

tools.iterateTapeTasks(taskList.slice(0, taskList.length), true, true, true).then(function (results) {
  console.log('sync done');
}).catch(function (e) {
  throw e;
});
