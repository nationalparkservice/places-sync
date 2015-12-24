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
  'CREATE TABLE tbl1(one VARCHAR(10), two SMALLINT);',
  "INSERT INTO tbl1 VALUES('hello!',10);",
  "INSERT INTO tbl1 VALUES('goodbye', 20);",
  'SELECT * FROM tbl1;',
  'DROP TABLE tbl1;'
];

t(testDB);
