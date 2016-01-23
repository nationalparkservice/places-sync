var createDatabase = require('../createDatabase');
var datawrap = require('datawrap');
var tape = require('tape');
var tools = require('../tools');
var fs = require('fs');
var guessFormat = require('../guessFormat');
var formats = tools.requireDirectory(__dirname + '/../formats');

var testSources = {
  load: {
    'loadCsvString': {
      'data': 'a,b,c\n1,2,3\n4,G,6'
    },
    'loadCsvFile': {
      'data': 'file:///csvDbtest2.csv'
    },
    'loadJsonString': {
      'data': JSON.stringify([{
        'a': 1,
        'b': 2,
        'c': 3
      }, {
        'a': 8,
        'b': 5,
        'c': 'a'
      }, {
        'a': 1.343,
        'b': 8,
        'c': 'q'
      }, {
        'a': 1.1000,
        'b': 2.0,
        'c': 'hi'
      }])
    }
  },
  addNew: {}
};

// Create the datasource
var createSourceDb = function (onLoadSources) {
  return createDatabase({
    'dataDirectory': './data/',
    data: onLoadSources
  });
};

var test = function (sourcesToTest) {
  tape('Testing createDatabase.js', function (t) {
    var sourceDb = createSourceDb(sourcesToTest.load);
    var sourceName;
    var tapeError = function (e) {
      t.end();
      reportError(e);
    };

    // Load the initial datasets
    var taskList = [{
      'name': 'Preload the initial sources',
      'task': sourceDb.load,
      'params': []
    }];

    // Add tests for the preloaded sources
    for (sourceName in sourcesToTest.load) {
      taskList.push({
        'name': 'Test Source ' + sourceName,
        'task': testSource,
        'params': [sourcesToTest.load[sourceName], sourceDb.sources, sourceName, t]
      });
    }

    // Add tests for the add-later sources
    for (sourceName in sourcesToTest.addNew) {
      // Add it to the database
      taskList.push({
        'name': 'Add Source ' + sourceName,
        'task': sourceDb.addData,
        'params': [sourcesToTest.addNew[sourceName], sourceDb.sources, sourceName, t]
      });

      // Add a test to make sure it worked
      taskList.push({
        'name': 'Test Source ' + sourceName,
        'task': testSource,
        'params': [sourcesToTest.addNew[sourceName], sourceDb.sources[sourceName], t]
      });
    }

    datawrap.runList(taskList).then(function (result) {
      console.log('success!');
      t.end();
    }).catch(tapeError);
  });
};

var reportError = function (e) {
  e = tools.arrayify(e);
  console.error(tools.readOutput(e));
  console.error(e[e.length - 1]);
  console.log('failure');
  throw e[e.length - 1];
};

var testSource = function (originalSource, databaseSources, sourceName, t) {
  return new datawrap.Bluebird(function (fulfill, reject) {
    var databaseSource = databaseSources[sourceName];
    var data;
    var fileRegExp = new RegExp('^file:///(.+?)$', 'g');

    // Select data from the source
    databaseSource.runQuery('SELECT * FROM "' + sourceName + '";', function (e, r) {
      if (e) {
        reject(e);
      } else {
        if (originalSource.data.match(fileRegExp)) {
          data = fs.readFileSync('./data/' + originalSource.data.replace(fileRegExp, '$1')).toString();
        } else {
          data = originalSource.data;
        }

        formats[guessFormat(data)]({
          'name': sourceName,
          'data': data
        }).then(function (transformedData) {
          transformedData.data.forEach(function (row, rowI) {
            row = Array.isArray(row) ? tools.addTitles(transformedData.columns.map(function (c) {
              return c.name;
            }), row) : row;
            for (var field in row) {
              t.equals(row[field].toString(), r[0][rowI][field].toString());
            }
          });
          fulfill('done');
        }).catch(function (e) {
          t.equals(1, 0);
          reject(e);
        });
      }
    });
  });
};

test(testSources);
