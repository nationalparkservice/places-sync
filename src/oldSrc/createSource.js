/* columns must contain the fields:
 *   primaryKey: primary key for the table (TODO: Support compound keys)
 *   columns: an array of all other columns to be updated
 *
 * columns can also contain the fields
 *   lastUpdated: field name for the last update (default UTC time, TODO: support more time options)
 *   removed: field name for the field that denotes if the record still exists (true means removed, TODO: support false as well)
 *
 */
var csvType = require('./sources/csv');
var Bluebird = datawrap.Bluebird;
var createSource = module.exports = function (sourceId, type) {
  var types = {
    'csv': csvType
  };
  var returnValue = types[type];
  returnValue.id = sourceId;
  return returnValue;
};


