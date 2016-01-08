var config = require('./config');
var datawrap = require('datawrap');
config = datawrap.fandlebars.obj(config, global.process);

var syncDb = datawrap(config.database.sync_db, config.database.defaults);

var t = function (sql) {
  syncDb.runQuery(sql)
    .then(function (e, r) {
      console.log(e, r);
    });
};

var testDB = [
  'CREATE TABLE sourcea (one VARCHAR(10), two SMALLINT);',
  "INSERT INTO  sourcea VALUES('hello!',10);",
  "INSERT INTO sourcea VALUES('goodbye', 20);",
  'SELECT * FROM sourcea ;',
  'DROP TABLE sourcea ;'
];

t(testDB);
