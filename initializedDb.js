var config = require('./config');
var tools = require('./src/tools');
var datawrap = require('datawrap');
config = datawrap.fandlebars.obj(config, global.process);
var syncDb = datawrap(config.database.sync_db, config.database.defaults);

var checkTable = "SELECT name FROM sqlite_master WHERE type='table' AND name='master';";
var createTable = 'file:///makeTable.sql';
// var dropTable = 'DROP TABLE master;';

syncDb.runQuery(checkTable)
  .then(function (r) {
    // console.log(r[0][0]);
    if (!r[0][0]) {
      syncDb.runQuery(createTable)
        .then(function (res) {
          console.log('Table Created!');
        })
        .catch(function (err) {
          console.error(tools.readOutput(err));
          throw (err[err.length - 1]);
        });
    } else {
      console.log('Table Already Initialized');
    }
  }).catch(function (e) {
    console.error(tools.readOutput(e));
    throw (e[e.length - 1]);
  });
