// Load that csv
var sources = require('./sources');
var iterateTasks = require('./tools/iterateTasks');

var csvConfig = {
  'connection': {
    'filePath': __dirname + '/' + 'test.csv',
    'type': 'csv'
  }
};
var thrower = function (e) {
  throw Array.isArray(e) ? e[e.length = 1] : e;
};

sources(csvConfig).then(function (source) {
  var tasks = [{
    'name': 'selectAll',
    'task': source.selectAll,
    'params': []
  }, {
    'name': 'selectLastUpdate',
    'task': source.selectLastUpdate,
    'params': []
  }];

  iterateTasks(tasks, 'test', true).then(console.log).catch(thrower);
}).catch(function (e) {
  throw Array.isArray(e) ? e[e.length - 1] : e;
});
