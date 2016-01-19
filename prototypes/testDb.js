var config = require('../config');
var tools = require('../src/tools');
var datawrap = require('datawrap');
var lzstring = require('lz-string');
var parseJson = require('../src/parseJson');
config = datawrap.fandlebars.obj(config, global.process);

/* config.database.test_db */
var testDb = datawrap({
  'name': 'test',
  'type': 'sqlite',
  'connection': ':memory:'
}, config.database.defaults);

var blob = function (d) {
  return d;
};

var geojson = JSON.stringify({
  'type': 'FeatureCollection',
  'features': [{
    'type': 'Feature',
    'properties': {},
    'geometry': {
      'type': 'Polygon',
      'coordinates': [
        [
          [-123.04687499999999,
            48.22467264956519
          ],
          [-124.45312499999999,
            40.713955826286046
          ],
          [-118.125,
            34.88593094075317
          ],
          [-110.390625,
            33.43144133557529
          ],
          [-104.4140625,
            31.353636941500987
          ],
          [-99.84374999999999,
            28.92163128242129
          ],
          [-97.3828125,
            24.846565348219734
          ],
          [-95.2734375,
            28.92163128242129
          ],
          [-92.10937499999999,
            30.14512718337613
          ],
          [-87.5390625,
            30.14512718337613
          ],
          [-84.0234375,
            30.751277776257812
          ],
          [-83.3203125,
            27.68352808378776
          ],
          [-80.15625,
            25.165173368663954
          ],
          [-80.15625,
            27.371767300523047
          ],
          [-80.85937499999999,
            30.751277776257812
          ],
          [-80.85937499999999,
            33.137551192346145
          ],
          [-76.640625,
            35.460669951495305
          ],
          [-75.5859375,
            37.996162679728116
          ],
          [-74.1796875,
            40.44694705960048
          ],
          [-72.421875,
            41.244772343082076
          ],
          [-71.12548828125,
            41.244772343082076
          ],
          [-69.9609375,
            41.73852846935917
          ],
          [-70.07080078125,
            42.08191667830631
          ],
          [-70.4443359375,
            41.983994270935625
          ],
          [-70.81787109374999,
            42.04929263868686
          ],
          [-70.9716796875,
            42.89206418807337
          ],
          [-70.6640625,
            43.61221676817573
          ],
          [-69.41162109375,
            44.008620115415354
          ],
          [-67.1484375,
            44.73112559264325
          ],
          [-67.19238281249999,
            45.120052841530544
          ],
          [-67.4560546875,
            45.537136680398596
          ],
          [-67.87353515625,
            45.75219336063106
          ],
          [-67.82958984375,
            46.965259400349275
          ],
          [-68.04931640625,
            47.338822694822
          ],
          [-68.84033203125,
            47.32393057095941
          ],
          [-69.169921875,
            47.47266286861342
          ],
          [-70.33447265624999,
            46.22545288226939
          ],
          [-70.3125,
            45.69083283645816
          ],
          [-70.6640625,
            45.30580259943578
          ],
          [-71.4111328125,
            45.1510532655634
          ],
          [-75.05859375,
            44.9336963896947
          ],
          [-76.9921875,
            43.70759350405294
          ],
          [-79.16748046874999,
            43.48481212891603
          ],
          [-78.85986328125,
            42.85985981506279
          ],
          [-82.59521484375,
            41.60722821271717
          ],
          [-83.232421875,
            42.35854391749705
          ],
          [-82.15576171875,
            43.48481212891603
          ],
          [-82.529296875,
            45.321254361171476
          ],
          [-84.88037109375,
            46.81509864599243
          ],
          [-88.57177734375,
            48.31242790407178
          ],
          [-94.74609375,
            48.748945343432936
          ],
          [-94.63623046875,
            49.42526716083716
          ],
          [-95.2294921875,
            49.42526716083716
          ],
          [-95.20751953125,
            48.922499263758255
          ],
          [-123.22265625000001,
            49.210420445650286
          ],
          [-123.04687499999999,
            48.22467264956519
          ]
        ]
      ]
    }
  }]
});

var columns = ['id', 'name', 'data'];
var testData = [
  [1, 'test1', blob(lzstring.compressToUTF16('["test1"]'))],
  [2, 'test2', blob(lzstring.compressToUTF16('{"test2":"test2"}'))],
  [3, 'test3', blob(lzstring.compressToUTF16('["test3"]'))],
  [4, 'test4', blob(lzstring.compressToUTF16(geojson))],
  [5, 'test5', blob(lzstring.compressToUTF16(geojson))],
  [6, 'test6', blob(lzstring.compressToUTF16(geojson))],
  [7, 'test7', blob(lzstring.compressToUTF16('["test7"]'))]
];
var queries = [
  // 'file:///makeTable.sql',
  // 'INSERT INTO master VALUES ("a","b","c",0,0,0);',
  // 'INSERT INTO master VALUES ("a","a","c",0,0,0);',
  // 'INSERT INTO master VALUES ("a","b","a",0,0,0);',
  // 'INSERT INTO master VALUES ("b","b","c",0,0,0);',
  // 'SELECT * FROM master;',
  // 'DROP TABLE master;',
  // 'CREATE TABLE sourcea (one VARCHAR(10), two SMALLINT);',
  // "INSERT INTO  sourcea VALUES('hello!',10);",
  // "INSERT INTO sourcea VALUES('goodbye', 20);",
  // 'SELECT * FROM sourcea ;',
  // 'DROP TABLE sourcea ;'
  ['CREATE TABLE "blob_test" ("id" INTEGER, "name" TEXT, "data" TEXT)'],
  ['INSERT INTO blob_test VALUES({{id}}, {{name}}, {{data}});', testData.map(function (d) {
    return tools.addTitles(columns, d);
  }), {
    'paramList': true
  }],
  ['SELECT * FROM blob_test'],
  [null, null, {
    'close': true
  }]
];

var t = function (sql) {
  var taskList = sql.map(function (q, i) {
    return {
      'name': 'Item ' + i,
      'task': testDb.runQuery,
      'params': q
    };
  });
  taskList.push({
    'name': 'Read value',
    'task': function (d) {
      return new datawrap.Bluebird(function (f, r) {
        var res = d[0].map(function (row, rowI) {
          return (['row' + (rowI+1), testData[rowI][2].length, JSON.stringify(parseJson(testData[rowI][2])) === JSON.stringify(parseJson(row.data)), testData[rowI][2] === row.data]);
        });
        f(res);
      });
    },
    'params': '{{Item 2}}'
  });
  datawrap.runList(taskList, '')
    .then(function (r) {
      console.error(tools.readOutput(r));
      console.log('done');
    }).catch(function (e) {
    console.error(tools.readOutput(e));
    throw (e[e.length - 1]);
  });
};

 t(queries);
