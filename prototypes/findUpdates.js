var datawrap = require('datawrap');
var datawrapDefaults = require('../defaults');
var defaults = datawrap.fandlebars.obj(datawrapDefaults, global.process);
var createDatabase = require('../src/createDatabase');

// A memory database used to bring in new data
var sourceDb = new createDatabase();

// The database used to do comparisons (could be stored in a file, but for this test, it is also in memory)
var compareDb = datawrap({
  'type': 'sqlite',
  'name': 'quickTest'
}, defaults);

var sources = [{
  'name': 'original',
  'data': [{
    'key': '0',
    'field': 'field0'
  }, {
    'key': '1',
    'field': 'field1'
  }, {
    'key': '2',
    'field': 'field2'
  }, {
    'key': '3',
    'field': 'field3'
  }, {
    'key': '4',
    'field': 'field4'
  }]
}, {
  'name': 'changed',
  'data': [{
    'key': '0',
    'field': 'field0'
  }, {
    'key': '1',
    'field': 'field1'
  }, {
    'key': '2',
    'field': 'field2'
  }, {
    'key': '3',
    'field': 'fieldChanged'
  }, {
    'key': '5',
    'field': 'field5'
  }]
}];

// Add these sources to sqlite
var commands = {
  // Create the compare tables
  'create': 'file:///makeTable.sql',
  // Basic inserts
  'insert': 'file:///insertData.sql',
  // New ids (in A or B, not in C)
  'findNew': 'file:///findCreated.sql',
  // Updated ids (a delete is treated as an update)
  'findUpdated': 'file:///findUpdated.sql',
  // Conflicting ids
  'findConflicts': 'file:///findConflicts.sql',
  'close': [null, null, {
    'close': true
  }]
};

var taskList = [{
  'name': 'load original dataset',
  'description': 'Loads the data for the original dataset',
  'task': sourceDb.addData,
  'params': [sources[0]]
}, {
  'name': 'load changed dataset',
  'description': 'Loads the data for the changed dataset',
  'task': sourceDb.addData,
  'params': [sources[1]]
}, {
  'name': 'create compare database',
  'description': 'Creates a blank database for comparing these datasets',
  'task': createCompareDb,
  'params': []
}, {
  'name': 'import original source hashes',
  'description': 'Imports the original dataset for a new compare task',
  'task': importOriginal,
  'params': ['{{create compare database}}', '{{load original dataset}}']
}, {
  'name': 'import changed source hashes',
  'description': 'Imports the changed dataset for a new compare task',
  'task': importNewVersion,
  'params': ['{{create compare database}}', '{{load changed dataset}}']
}, {
  'name': 'find created records',
  'description': 'Determine which records are newly created',
  'task': findCreatedRecords,
  'params': ['{{importToCompare}}', processName, editDate]
}, {
  'description': 'Determine which records have been updated',
  'task': findUpdatedRecords,
  'params': ['{{importToCompare}}', processName, editDate]
}, {
  'name': 'find deleted records',
  'description': 'Determine which records have been deleted',
  'task': findDeletedRecords,
  'params': ['{{importToCompare}}', processName, editDate]
}, {
  'name': 'get record data',
  'description': 'Goes back to the data source and pulls the values for new and created records',
  'task': getRecordData,
  'params': ['{{find created records}}', '{{find deleted records}}', '{{find deleted records}}']
}, {
  'name': 'transform new records to places',
  'description': 'Transforms the output from the source to something usable in places',
  'task': sourceToPlaces,
  'params': ['{{getRecordData}}']
}, {
  'name': 'write to places',
  'description': 'writes creates, updates, and deletes to places',
  'task': writeToPlaces,
  'params': ['{{sourceToPlaces}}']
}];
