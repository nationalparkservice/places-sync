var datawrap = require('datawrap');
var datawrapDefaults = require('./defaults');
var fandlebars = datawrap.fandlebars;
var defaults = datawrap.fandlebars.obj(datawrapDefaults, global.process);
var md5 = require('./md5');

var datasets = {
  sourceA: [
    ['0', 'quickTest', '0', md5('test0')],
    ['1', 'quickTest', '0', md5('test1')],
    ['2', 'quickTest', '0', md5('test2')],
    ['3', 'quickTest', '0', md5('test3')],
    ['4', 'quickTest', '0', md5('test4')],
    ['5', 'quickTest', '0', md5('test5')]
  ],
  sourceB: [
    ['0', 'quickTest', '0', md5('test0')],
    ['1', 'quickTest', '0', md5('test1')],
    ['2', 'quickTest', '0', md5('test2')],
    ['3', 'quickTest', '0', md5('test3')],
    ['4', 'quickTest', '0', md5('test4')],
    ['5', 'quickTest', '0', md5('test6')] // this one is different!
  ]
};

// Add these sources to sqlite
var commands = {
  'createA': 'CREATE TABLE sourcea (id numeric, task_name text, last_update numeric, hash text);',
  'createB': 'CREATE TABLE sourceb (id numeric, task_name text, last_update numeric, hash text);',
  'createC': 'CREATE TABLE sourcec (id numeric, task_name text, last_update numeric, hash text);',
  'insertA': "INSERT INTO sourcea (id, task_name, last_update, hash) VALUES ('{{0}}', '{{1}}', '{{2}}', '{{3}}');",
  'insertB': "INSERT INTO sourceb (id, task_name, last_update, hash) VALUES ('{{0}}', '{{1}}', '{{2}}', '{{3}}');",
  'insertC': "INSERT INTO sourcec (id, task_name, last_update, hash) VALUES ('{{0}}', '{{1}}', '{{2}}', '{{3}}');"
};

var row = 0;
var sql = [
  commands.createA,
  commands.createB,
  commands.createC
];
for (row = 0; row < datasets.sourceA.length; row++) {
  sql.push(fandlebars(commands.insertA, datasets.sourceA[row]));
}
for (row = 0; row < datasets.sourceB.length; row++) {
  sql.push(fandlebars(commands.insertB, datasets.sourceB[row]));
}

// Set up a db in memory for this
var db = datawrap({
  'type': 'sqlite'
}, defaults);


var re = new RegExp('(CREATE|INSERT).+?(source.).+', 'g');
var taskList = sql.map(function (statement, i) {
  return {
    'name': statement.replace(re, '$1_$2' + i),
    'task': db.runQuery,
    'params': [statement]
  };
});
db.runQuery(sql)
  .then(function (a) {
    console.log(JSON.stringify(a[a.length - 1]));
  }).catch(function (e) {
    throw e;
  });

/*
datawrap.runList(taskList.slice, 'Create Task')
  .then(function (a) {
    console.log(JSON.stringify(a[a.length - 1]));
  }).catch(function (e) {
    throw e;
  });
*/
