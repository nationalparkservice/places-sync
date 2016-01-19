var datawrap = require('datawrap');

var test = module.exports = function (connection) {
  var importSource = function (process, source) {
    return new datawrap.Bluebird(function (fulfill, reject) {
      var fromDate = 0;
      // TODO there will need to be a tool that makes a source from a database and table
      source.getHashedData(fromDate)
        .then(function (hashedData) {
          var insertData = hashedData.map(function (row) {
            return {
              'key': row.key,
              'process': process,
              'source': source.id,
              'hash': row.hash,
              'last_update': row.lastUpdate,
              'removed': row.removed
            };
          });
          connection.runQuery('file:///insertData.sql', insertData, {
            'paramList': true
          }).then(function (inserts) {
            fulfill(true);
          }).catch(function (error) {
            reject(error);
          });
        })
        .catch(function (error) {
          reject(error);
        });
    });
  };
  return {
    initializeSource: function (process, sourceA, sourceB) {
      return new datawrap.Bluebird(function (f, r) {
        // Make sure we have the master table
        // Make sure we don't have any previous data for this process
        // Maybe ask if we can wipe it?
        //
        // import sourceA
        sourceA.getHashedData(0).then(f).catch(r);
      // import sourceB
      });
    }
  };
};

test({
  runQuery: function (a, b, c) {
    return new datawrap.Bluebird(function (f, r) {
      f(a, b, c);
    });
  }
}).initializeSource('test', {
  'id': 'TestSource',
  getHashedData: function (start) {
    return new datawrap.Bluebird(function (f, r) {
      f([{
        'key': 1,
        'hash': 'sgewagwf4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 2,
        'hash': 'sgewagsf4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 3,
        'hash': 'sgewagof4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 4,
        'hash': '5gewagef4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 5,
        'hash': 'sgewagek4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 6,
        'hash': 'sgewagef4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 7,
        'hash': 'sgewaref4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 8,
        'hash': 'sgewahef4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }, {
        'key': 9,
        'hash': 'sgewajef4v34q34fc4',
        lastUpdate: 0,
        removed: 0
      }]);
    });
  }
}).then(function (result) {
  console.log(result);
}).catch(function (err) {
  console.log(
    'err', err);
});
