var sync = require('./index');

var altUnitCodeProcess = 'places_boundaries_alt_unit_codes_v2';
var altUnitCodesPostgresql = {
  'name': 'places_boundaries_alt_unit_codes_postgresql',
  'connection': {
    'type': 'postgresql',
    'processName': altUnitCodeProcess,
    'host': 'localhost',
    'user': 'postgres',
    'password': 'postgres',
    'database': 'places_boundaries_v2',
    'table': 'alt_unit_codes'
  },
  'fields': {
    'primaryKey': 'unit_id',
    'lastUpdated': 'updated_at',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined
  }
};

var altUnitCodesCartoDB = {
  'name': 'places_boundaries_alt_unit_codes_cartodb',
  'connection': {
    'type': 'cartodb',
    'processName': altUnitCodeProcess,
    'account': 'nps',
    'apiKey': '5e9f65121aacd18d92a8f79be65f404e7aba8cf6',
    'table': 'alt_unit_codes_v2'
  },
  'fields': {
    'primaryKey': 'unit_id',
    'lastUpdated': 'updated_at',
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

var parksProcessName = 'parks_v2_sync';
var parksPostgresql = {
  'name': 'places_boundaries_v2_parks_postgresql',
  'connection': {
    'type': 'postgresql',
    'processName': parksProcessName,
    'host': 'localhost',
    'user': 'postgres',
    'password': 'postgres',
    'database': 'places_boundaries_v2',
    'table': 'parks'
  },
  'fields': {
    'primaryKey': 'unit_id',
    'lastUpdated': 'updated_at',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined
  }
};

var parksCartoDb = {
  'name': 'places_boundaries_v2_parks_cartodb',
  'connection': {
    'type': 'cartodb',
    'processName': parksProcessName,
    'account': 'nps',
    'apiKey': '5e9f65121aacd18d92a8f79be65f404e7aba8cf6',
    'table': 'parks_v2'
  },
  'fields': {
    'primaryKey': 'unit_id',
    'lastUpdated': 'updated_at',
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

var parksLabelProcessName = 'parks_v2_label_sync';
var parksLabelPostgresql = {
  'name': 'places_boundaries_v2_parks_label_postgresql',
  'connection': {
    'type': 'postgresql',
    'processName': parksLabelProcessName,
    'host': 'localhost',
    'user': 'postgres',
    'password': 'postgres',
    'database': 'places_boundaries_v2',
    'table': 'parks_label'
  },
  'fields': {
    'primaryKey': 'unit_id',
    'lastUpdated': 'updated_at',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined
  }
};

var parksLabelCartoDb = {
  'name': 'places_boundaries_v2_parks_label_cartodb',
  'connection': {
    'type': 'cartodb',
    'processName': parksLabelProcessName,
    'account': 'nps',
    'apiKey': '5e9f65121aacd18d92a8f79be65f404e7aba8cf6',
    'table': 'parks_label_v2'
  },
  'fields': {
    'primaryKey': 'unit_id',
    'lastUpdated': 'updated_at',
    'removed': undefined,
    'removedValue': undefined,
    'forced': undefined,
    'mapped': {
      'cartodb_id': null,
      'the_geom': 'geom_label',
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
    'lastUpdated': 'last_updated',
    'hash': 'hash',
    'removed': 'is_removed',
    'removedValue': 1,
    'forced': undefined
  }
};

// sync(masterCacheConfig, altUnitCodesPostgresql, altUnitCodesCartoDB, true).then(function (r) {
sync(masterCacheConfig, parksLabelPostgresql, parksLabelCartoDb, false).then(function (rr) {
  console.log('sync done', rr);
}).catch(function (e) {
  throw e;
});
// }).catch(function (e) {
// throw e;
// });
