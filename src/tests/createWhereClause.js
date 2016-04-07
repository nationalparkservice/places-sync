var tape = require('tape');
var fandlebars = require('datawrap').fandlebars;
var createWhereClause = require('../createWhereClause');

var tests = [{
  'name': 'Test simple eq',
  'task': createWhereClause,
  'params': [{
    value: {
      $eq: 5
    }
  }],
  'expected': '("value" = 5)',
  'rawExpected': ['("value" = {{5d1ea2664d43ace853b3b7968bf428f7}})', {
    '5d1ea2664d43ace853b3b7968bf428f7': 5
  }]
}, {
  'name': 'Test simple eq with a gt',
  'task': createWhereClause,
  'params': [{
    value: {
      $eq: 5
    },
    'otherValue': {
      $gte: 10
    }
  }],
  'expected': '("value" = 5 AND "otherValue" >= 10)'
}, {
  'name': 'Test array',
  'task': createWhereClause,
  'params': [{
    value: {
      $ne: 5
    },
    'otherValue': ['a', 'b', 'c', 'd', 'e', 'f', 'g']
  }],
  'expected': '("value" != 5 AND ("otherValue" = a OR "otherValue" = b OR "otherValue" = c OR "otherValue" = d OR "otherValue" = e OR "otherValue" = f OR "otherValue" = g))'
}, {
  'name': 'Test array',
  'task': createWhereClause,
  'params': [{
    value: 5,
    otherValue: 'text'
  }],
  'expected': '("value" = 5 AND "otherValue" = text)'
}, {
  'name': 'Test array',
  'task': createWhereClause,
  'params': [{
    value: 5,
    otherValue: ['text']
  }],
  'expected': '("value" = 5 AND ("otherValue" = text))'
}, {
  'name': 'Test array',
  'task': createWhereClause,
  'params': [{
    value: 5,
    otherValue: {
      '$eq': 'text'
    }
  }],
  'expected': '("value" = 5 AND "otherValue" = text)'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    name: {
      $like: '%ooth%'
    }
  }],
  'expected': '("name" LIKE %ooth%)'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    'school': 'Woodstock Music School'
  }],
  'expected': '("school" = Woodstock Music School)'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    active: true
  }],
  'expected': '("active" = true)'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    id: [1, 2, 3]
  }],
  'expected': '(("id" = 1 OR "id" = 2 OR "id" = 3))'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    'value': 'a',
    '$or': [{
      'id': ['q', 'w', 'e']
    }, {
      'id': {
        '$ne': 'r'
      }
    }]
  }],
  'expected': '("value" = a AND (("id" = q OR "id" = w OR "id" = e) OR "id" != r))'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    name: 'a project',
    id: {
      $or: [
        [1, 2, 3], {
          $gt: 10
        }
      ]
    }
  }],
  'expected': '("name" = a project AND (("id" = 1 OR "id" = 2 OR "id" = 3) OR "id" > 10))'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    active: true
  }],
  'expected': '("active" = true)'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    'value': 'a',
    '$and': [{
      'id': ['q', 'w', 'e']
    }, {
      'id': {
        '$ne': 'r'
      }
    }]
  }],
  'expected': '("value" = a AND (("id" = q OR "id" = w OR "id" = e) AND "id" != r))'
}, {
  'name': 'Tests from sequelizejs examples',
  'task': createWhereClause,
  'params': [{
    name: 'a project',
    id: {
      $and: [
        [1, 2, 3], {
          $gt: 10
        }
      ]
    }
  }],
  'expected': '("name" = a project AND (("id" = 1 OR "id" = 2 OR "id" = 3) AND "id" > 10))'
}, {
  'name': 'Tests the available columns',
  'task': createWhereClause,
  'params': [{
    name: 'a project',
    id: {
      $and: [
        [1, 2, 3], {
          $gt: 10
        }
      ]
    }
  },['name']],
  'expected': '("name" = a project)'
}, {
  'name': 'Tests the available columns',
  'task': createWhereClause,
  'params': [{
    name: 'a project',
    id: {
      $and: [
        [1, 2, 3], {
          $gt: 10
        }
      ]
    }
  },['id']],
  'expected': '((("id" = 1 OR "id" = 2 OR "id" = 3) AND "id" > 10))'
}];

var runTests = function(tests) {
  tape('Check where clauses', function(t) {
    for (var i = 0; i < tests.length; i++) {
      var rawResult = tests[i].task.apply(this, tests[i].params);
      var result = fandlebars.apply(this, rawResult);
      console.log('result for', tests[i].name, '→', result);
      if (tests[i].rawExpected) {
        t.deepEqual(tests[i].rawExpected, rawResult);
      }
      t.equal(tests[i].expected, result);
    }
    t.end();
  });
};

runTests(tests);
