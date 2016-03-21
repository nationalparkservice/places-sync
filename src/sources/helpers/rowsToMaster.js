var columnsToKeys = require('./columnsToKeys');
var keyCombine = require('./keyCombine');
var md5 = require('../../tools/md5');
var simplifyArray = require('../../tools/simplifyArray');

var rowsToMaster = function (rows, columns, sourceName, processName, removeRow) {
  var keys = columnsToKeys(columns);
  return rows.map(function (row) {
    return {
      'key': keyCombine(keys.primaryKeys, row),
      'process': processName || 'sync',
      'source': sourceName,
      'hash': md5(keyCombine(simplifyArray(columns), row)),
      'last_updated': parseFloat(row[keys.lastUpdatedField], 10), // Make sure we only add a number here
      'is_removed': removeRow ? 1 : 0
    };
  });
};

module.exports = rowsToMaster;
