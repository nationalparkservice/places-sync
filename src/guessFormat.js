module.exports = function (data) {
  // Guesses between CSV, JSON, and GeoJSON
  var dataFormat;
  var tmp;
  var isGeojson = function (d) {
    return data.type && data.type === 'FeatureCollection';
  };

  if (typeof data === 'object') {
    dataFormat = isGeojson(data) ? 'geojson' : 'json';
  } else if (typeof data === 'string') {
    try {
      tmp = JSON.parse(data);
      dataFormat = isGeojson(tmp) ? 'geojson' : 'json';
    } catch (error) {
      dataFormat = 'csv';
    }
  }
  return dataFormat;
};
