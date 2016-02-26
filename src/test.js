// Load that csv
var sources = require('./sources');

var csvConfig = {
  'connection': {
    'filePath': __dirname + '/' + 'test.csv',
    'type': 'csv'
  }
};
var thrower = function (e) {
  throw e;
};

sources(csvConfig).then(function (source) {
  console.log(source);
  source.selectAll().then(console.log).catch(thrower);
}).catch(function (e) {
  throw Array.isArray(e) ? e[e.length - 1] : e;
});
