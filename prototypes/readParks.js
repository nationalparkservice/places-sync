var CreateSource = require('../src/createSource');
var datawrap = require('datawrap');
var Bluebird = datawrap.Bluebird;

var parksSource = {
  'connection': {
    "address": "HOSTNAME",
    "dbname": "DATABASE_NAME",
    "password": "PASSWORD",
    "username": "USERNAME",
  },
  'data': 'file:///parks/getParks.sql',
  'extractionType': 'json',
  'format': 'database',
  'name': 'psql_parks',
  'primaryKey': 'unit_id'
};

var sourceDb = new CreateSource();

var getData = function(source, db) {
  console.log('ok');
};

getData(parksSource, sourceDb);
