var config = require('../config');
var datawrap = require('datawrap');
config = datawrap.fandlebars.obj(config, global.process);

var syncDb = datawrap(config.database.test_db, config.database.defaults);

var t = function (sql) {
  syncDb.runQuery(sql)
    .then(function (e, r) {
      console.log(e, r);
    }).catch(function(e){
      console.error(tools.readOutput(e));
    });

};

var testDB = [
  'file:///makeTable.sql',
  'INSERT INTO master VALUES ("a","b","c",0,0,0);',
  'INSERT INTO master VALUES ("a","a","c",0,0,0);',
  'INSERT INTO master VALUES ("a","b","a",0,0,0);',
  'INSERT INTO master VALUES ("b","b","c",0,0,0);',
  'SELECT * FROM master;',
  'DROP TABLE master;',
  'CREATE TABLE sourcea (one VARCHAR(10), two SMALLINT);',
  "INSERT INTO  sourcea VALUES('hello!',10);",
  "INSERT INTO sourcea VALUES('goodbye', 20);",
  'SELECT * FROM sourcea ;',
  'DROP TABLE sourcea ;'
];

t(testDB);
