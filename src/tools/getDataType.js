module.exports = function (value, maxType, dataType) {
  var typeHierarchy = ['text', 'float', 'bigint', 'integer'];
  var passes = 0;
  var type;
  value = value && value.toString ? value.toString() : value;
  if (value && !(isNaN(value) || value.replace(/ /g, '').length < 1) && maxType !== 'text' && dataType !== 'string') {
    passes += 1; // this might be a float
    if (parseFloat(value, 10) === parseInt(value, 10) && maxType !== 'float') {
      passes += 1; // This is an int!
      if (Math.abs(parseInt(value, 10)) < 2147483646) {
        passes += 1;
      }
    }
  }
  type = typeHierarchy[passes];
  return value ? type : maxType;
};
