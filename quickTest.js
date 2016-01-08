var datawrap = require('datawrap');
var datawrapDefaults = require('./defaults');
var fandlebars = datawrap.fandlebars;
var defaults = datawrap.fandlebars.obj(datawrapDefaults, global.process);
var guid = require('./src/guid');
var md5 = require('./src/md5');
var guids = ['','','','',''].map(function(){return guid();});

var datasets = {
  sourceA: [
    [guids['0'], 'quickTest', '0', '0', md5('test0')],
    [guids['1'], 'quickTest', '0', '0', md5('test1')],
    [guids['2'], 'quickTest', '0', '0', md5('test2')],
    [guids['3'], 'quickTest', '0', '0', md5('test3')],
    [guids['4'], 'quickTest', '0', '0', md5('test4')],
    [guids['5'], 'quickTest', '0', '0', md5('test5')]
  ],
  sourceB: [
    [guids['0'], 'quickTest', '1', '0', md5('test0')],
    [guids['1'], 'quickTest', '1', '0', md5('test1')],
    [guids['2'], 'quickTest', '1', '0', md5('test2')],
    [guids['3'], 'quickTest', '1', '0', md5('test3')],
    [guids['4'], 'quickTest', '1', '0', md5('test4')],
    [guids['5'], 'quickTest', '1', '0', md5('test6')] // this one is different!
  ]
};

// Add these sources to sqlite
var commands = {
  // Create the compare tables
  'createA': 'CREATE TABLE sourceA (id text, task_name text, last_update numeric, removed numeric, hash text);',
  'createB': 'CREATE TABLE sourceB (id text, task_name text, last_update numeric, removed numeric, hash text);',
  'createC': 'CREATE TABLE sourceC (id text, task_name text, last_update numeric, removed numeric, hash text);',
  // Basic inserts
  'insertA': "INSERT INTO sourceA (id, task_name, last_update, hash) VALUES ('{{0}}', '{{1}}', '{{2}}', '{{3}}'), '{{4}}';",
  'insertB': "INSERT INTO sourceB (id, task_name, last_update, hash) VALUES ('{{0}}', '{{1}}', '{{2}}', '{{3}}'), '{{4}}';",
  'insertC': "INSERT INTO sourceC (id, task_name, last_update, hash) VALUES ('{{0}}', '{{1}}', '{{2}}', '{{3}}'), '{{4}}';",
  // New ids (in A or B, not in C)
  'findNewFromA': "SELECT source.id FROM sourceA source LEFT OUTER JOIN sourceC master ON source.id = master.id AND source.task_name = master.task_name WHERE source.task_name = 'test' AND master.id IS NULL;",
  'findNewFromB': "SELECT source.id FROM sourceB source LEFT OUTER JOIN sourceC master ON source.id = master.id AND source.task_name = master.task_name WHERE source.task_name = 'test' AND master.id IS NULL;",
  // Updated ids
  'findRemoved': 'SELECT master.id FROM sourceC master LEFT OUTER JOIN sourceB b ON a.id = b.id WHERE a.hash != b.hash;',
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
    'name': statement.replace(re, '$1_$2'),
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

