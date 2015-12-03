var dat = require('dat-core');
// var db = dat('./places-sync');
var fs = require('fs');
var http = require('http');
var source = JSON.parse(fs.readFileSync('sources.json', 'utf8'))[0];

http.get(source.feature_service + 'query?f=json&layerDefs=0:' + source.unit_code_key + '=\'' + source.unit_code + '\'&outSR=4326', function (response) {
  response
    .pipe(fs.createWriteStream('test.json'));
    /*
    .pipe(db.createWriteStream({
      dataset: 'buildings'
    }));
    */
});

  /*
  .pipe(select('layers[0]'))
  .pipe(through(function (o, enc, next) {
    console.log(o);
    var properties = o.properties;
    properties.coordinates = o.geometry.coordinates;
    properties.key = o.properties[source.source_system_key];
    console.log(properties);
    next(null, properties);
  }))
  */
  /*
  .pipe(ldj.serialize())
  .pipe(dat('./_dat/buildings').createWriteStream({
    primary: 'key'
  }));
  */

// console.log(stream);

// request(source.feature_service + 'query?layerDefs=0:' + source.unit_code_key + '=\'' + source.unit_code + '\'&outSR=4326&returnGeometry=true&f=json', function (error, response, body) {
  // if (!error && response.statusCode === 200) {
    // var features = JSON.parse(body).layers[0].features;
    // var path = './_temp/' + new Date().getTime() + '.json';

    // console.log(features);
    // fs.writeFile(path, JSON.stringify(features));
    // dat.import(path);
    // dat('./_data/buildings').import(path);

    /*
    var geojson = {
      features: [],
      type: 'FeatureCollection'
    };

    for (var i = 0; i < features.length; i++) {
      var feature = arcgisParser.parse(features[i]);

      feature.id = feature.properties[source.source_system_key];
      geojson.features.push(feature);
    }

    path = './_temp/' + new Date().getTime() + '.geojson';
    fs.writeFile(path, JSON.stringify(geojson));
    dat.import(path);
    /*
    setTimeout(function () {
      fs.unlink(path);
    }, 3000);
    */
  // }
// });
