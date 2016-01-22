// TODO This test doesn't use tape
var geojsonToJsonTable = require('../geojsonToJsonTable');
var geojson = require('./data/geojsonTest.json');

console.log(JSON.stringify(geojsonToJsonTable(geojson), null, 2));
