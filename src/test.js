// Load that csv
var sources = require('./sources');

var csvConfig = {
  'connection': {
    'filePath': __dirname + '/' + 'test.csv',
    'type': 'csv'
  }
};

sources(csvConfig).then(console.log).catch(function (e) {
  throw Array.isArray(e) ? e[e.length - 1] : e;
});
