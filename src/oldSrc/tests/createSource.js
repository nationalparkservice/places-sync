var createSource = require('../createSource');

var csvSource = createSource('testFile', 'csv');
csvSource('file:///tests/csv/mergeSourceA.csv', {
  'primaryKey': 'id',
  'columns': ['field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9', 'field10']
})
  .then(function (source) {
    source.getHashedData()
      .then(console.log)
      .catch(console.error);
  })
  .catch(console.error);
