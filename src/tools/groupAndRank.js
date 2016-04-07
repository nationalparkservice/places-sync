var simplifyArray = require('./simplifyArray');
var arrayify = require('./arrayify');

module.exports = function (data, groupBy, rank, returnArrayOf) {
  // If return Arrays is true, it will return only three values in the object
  // { groupBy:, rank: {returnArrayOf}: [] }
  var newArray = [];
  var i, filtered, dups;
  data = JSON.parse(JSON.stringify(data));
  for (i = data.length - 1; i >= 0; i--) {
    if (simplifyArray(newArray, groupBy).indexOf(data[i][groupBy]) === -1) {
      filtered = data.filter(function (a) {
        return a[groupBy] === data[i][groupBy];
      });
      dups = filtered.sort(function (a, b) {
        return a[rank] - b[rank];
      })[0];
      if (returnArrayOf) {
        arrayify(returnArrayOf).map(function (field) {
          dups[field] = filtered.filter(function (value) {
            return value[rank] === dups[rank];
          }).map(function (innerValues) {
            return innerValues[field];
          });
        });
      }
      newArray.push(dups);
    }
  }
  return newArray;
};
