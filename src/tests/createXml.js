var CreateDatabase = require('../createDatabase');

var xmlSource = {
  'name': 'nyc_kayaks',
  'data': 'http://www.nycgovparks.org/bigapps/DPR_Kayak_001.xml',
  'root': 'kayak.facility',
  'geometryColumns': [{
    'name': 'lat',
    'type': 'latitude',
    'projection': 'ESPG:4326'
  }, {
    'name': 'lon',
    'type': 'longitude',
    'projection': 'ESPG:4326'
  }],
  'primaryKey': 'Prop_ID',
  'format': 'xml'
};

var geojsonDestination = {
  'name': 'nyc_kayak_geojson',
  'data': 'file:///nyc_kayaks.geojson',
  'format': 'geojson'
};

var test = function () {
  var sourceDb = new CreateDatabase();
  sourceDb.addData(xmlSource).then(function (r) {
    console.log(r);
    r.export(geojsonDestination, function (e, r) {
      console.log(e, r);
      if (e) throw e;
    });
  }).catch(function (e) {
    throw e
  });
};

test();
