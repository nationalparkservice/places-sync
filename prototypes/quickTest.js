var datawrap = require('datawrap');
var datawrapDefaults = require('../defaults');
var defaults = datawrap.fandlebars.obj(datawrapDefaults, global.process);
var guid = require('../src/guid');
var md5 = require('../src/md5');
var guids = ['', '', '', '', ''].map(function () {
  return guid();
});

var datasets = {
  sourceA: [
    [guids['0'], 'quickTest', 'sourceA', md5('test1'), '0', '0'],
    [guids['1'], 'quickTest', 'sourceA', md5('test2'), '0', '0'],
    [guids['2'], 'quickTest', 'sourceA', md5('test3'), '0', '0'],
    [guids['3'], 'quickTest', 'sourceA', md5('test4'), '0', '0'],
    [guids['4'], 'quickTest', 'sourceA', md5('test5'), '0', '0']
  ],
  sourceB: [
    [guids['0'], 'quickTest', 'sourceB', md5('test1'), '0', '0'],
    [guids['1'], 'quickTest', 'sourceB', md5('test2'), '0', '0'],
    [guids['2'], 'quickTest', 'sourceB', md5('test3'), '0', '0'],
    [guids['3'], 'quickTest', 'sourceB', md5('test4'), '0', '0'],
    [guids['4'], 'quickTest', 'sourceB', md5('test5'), '0', '0']
  ],
  columns: [
    'key', 'process', 'source', 'hash', 'last_update', 'removed'
  ]
};

// Add these sources to sqlite
var commands = {
  // Create the compare tables
  'create': 'file:///makeTable.sql',
  // Basic inserts
  'insert': 'file:///insertData.sql',
  // New ids (in A or B, not in C)
  'findNew': 'file:///findCreated.sql',
  // Updated ids (a delete is treated as an update)
  'findUpdated': 'file:///findUpdated.sql',
  // Conflicting ids
  'findConflicts': 'file:///findConflicts.sql',
  'close': [null, null, {
    'close': true
  }]
};

var addTitles = function (titles, data) {
  var returnValue = {};
  titles.forEach(function (title, index) {
    returnValue[title] = data[index];
  });
  return returnValue;
};

// Set up a db in memory for this
var db = datawrap({
  'type': 'sqlite',
  'name': 'quickTest'
}, defaults);

var taskList = [{
  'name': 'Create Database',
  'task': db.runQuery,
  'params': [
    commands.create, {}
  ]
}, {
  'name': 'Insert Source A',
  'task': db.runQuery,
  'params': [commands.insert,
    datasets.sourceA.map(function (row) {
      return addTitles(datasets.columns, row);
    }), {
      'paramList': true
    }
  ]
}, {
  'name': 'Insert Source B',
  'task': db.runQuery,
  'params': [commands.insert,
    datasets.sourceB.map(function (row) {
      return addTitles(datasets.columns, row);
    }), {
      'paramList': true
    }
  ]
}];

taskList.push({
  'name': 'Find New',
  'task': db.runQuery,
  'params': commands.close
});

datawrap.runList(taskList, 'Main Task')
  .then(function (a) {
    console.error(readOutput(a));
    console.log('success');
  }).catch(function (e) {
    console.error(readOutput(e));
    console.log('failure');
    throw e[e.length - 1];
  });

var readOutput = function (output) {
  return JSON.stringify([].concat(output).map(function (o) {
    return o.toString() + (o.toString().substr(0, 5) === 'Error' ? '\\n' + o.stack : '');
  }), null, 2).replace(/\\n/g, '\n');
};
