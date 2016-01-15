var datawrap = require('datawrap');
var datawrapDefaults = require('./defaults');
var fandlebars = datawrap.fandlebars;
var defaults = datawrap.fandlebars.obj(datawrapDefaults, global.process);
var guid = require('./src/guid');
var md5 = require('./src/md5');
var guids = ['', '', '', '', ''].map(function () {
  return guid();
});

var datasets = {
  sourceA: [
    [guids['0'], 'quickTest', 'sourceA', md5('test1'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceA', md5('test2'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceA', md5('test3'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceA', md5('test4'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceA', md5('test5'), '0', '0']
  ],
  sourceB: [
    [guids['0'], 'quickTest', 'sourceB', md5('test1'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceB', md5('test2'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceB', md5('test3'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceB', md5('test4'), '0', '0'],
    [guids['0'], 'quickTest', 'sourceB', md5('test5'), '0', '0']
  ]
};

// Add these sources to sqlite
var commands = {
  // Create the compare tables
  'create': 'file:///makeTables.sql',
  // Basic inserts
  'insert': 'file:///insertData.sql',
  // New ids (in A or B, not in C)
  'findNew': 'file:///findCreated.sql',
  // Updated ids (a delete is treated as an update)
  'findUpdated': 'file:///findUpdated.sql',
  // Conflicting ids
  'findConflicts': 'file:///findConflicts.sql',
  'close': 'close'
};

var row = 0;
var sql = [
  commands.create
];

for (row = 0; row < datasets.sourceA.length; row++) {
  sql.push(fandlebars(commands.insert, datasets.sourceA[row]));
}
for (row = 0; row < datasets.sourceB.length; row++) {
  sql.push(fandlebars(commands.insert, datasets.sourceB[row]));
}

// Set up a db in memory for this
var db = datawrap({
  'type': 'sqlite',
  'name': 'quickTest'
}, defaults);

var re = new RegExp('(CREATE|INSERT).+?(source.).+', 'g');
var taskList = [{
  'name': 'Create Database',
  'task': db.runQuery,
  'params': [
    [commands.createA, commands.createB, commands.createC]
  ]
}, {
  'name': 'Create Database',
  'task': db.runQuery,
  'params': function () {
    var sql = [];
    for (row = 0; row < datasets.sourceA.length; row++) {
      sql.push(fandlebars(commands.insertA, datasets.sourceA[row]));
    }
    for (row = 0; row < datasets.sourceB.length; row++) {
      sql.push(fandlebars(commands.insertB, datasets.sourceB[row]));
    }
    return [sql];
  }()
}];

taskList.push({
  'name': 'Find New',
  'task': db.runQuery,
  'params': commands.findNew
});

taskList.push({
  'name': 'Find New',
  'task': db.runQuery,
  'params': commands.close
});

datawrap.runList(taskList, 'Main Task')
  .then(function (a) {
    console.log(JSON.stringify(a[a.length - 1]));
    console.log('success');
  }).catch(function (e) {
    console.log(e);
    console.log(e.stack);
    console.log('failure');
    throw e;
});

/*
db.runQuery(sql)
  .then(function (a) {
    console.log(JSON.stringify(a[a.length - 1]));
  }).catch(function (e) {
  throw e;
});*/
