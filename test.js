// This will make a database completely in memory and try to merge it

var currentTime = function () {
  return new Date().getTime();
};

// First we need to set up the prerequisite database
var createBlankDatabase = function () {
  var taskSQL = 'CREATE TABLE tasks (task_name TEXT, last_update NUMERIC, conflict_count INTEGER, locked INTEGER) PRIMARY KEY task_name UNIQUE NOT NULL;';
  var log = 'CREATE TABLE log (id TEXT, task_name TEXT, last_update NUMERIC, hash TEXT, row_data BLOB) PRIMARY KEY id, task_name UNIQUE NOT NULL;';
// Somehow run this
};

var addTask = function (taskName) {
  var replaceObj = {
    taskName: taskName,
    currentTime: currentTime
  };

  var checkForTask = 'SELECT count(*) (SELECT task_name FROM tasks WHERE task_name = {{taskName}} UNION ALL SELECT task_name FROM log WHERE task_name = {{taskName}});';
  var addTask = 'INSERT INTO tasks (task_name, last_update, conflict_count, locked) VALUES ({{taskName}}, {{currentTime}}, 0, 0);';

// Somehow run this
};

var createDummyData = function (tableName, dataTable) {
  var replaceObj = dataTable || [];
  var rows = typeof dataTable === 'number' ? dataTable : typeof dataTable === 'object' ? dataTable.length : 0;
  var fields = typeof dataTable === 'object' ? Object.keys(dataTable[0]).length : Math.floor(Math.random() * 16) + 3;
  var getFieldNames = function (d) {
    var returnValue = [];
    for (var field in d[0]) {
      returnValue.push(field);
    }
    returnValue;
  };
  var tempObj = {};
  if (!dataTable) {
    for (var i = 0; i < rows; i++) {
      tempObj = {};
      for (var j = 0; j < fields; j++) {
        tempObj['field' + j] = Math.random().toString(35).substr(-25);
      }
      replaceObj.push(tempObj);
    }
  }
  var createTable = 'CREATE TABLE "' + tableName + '" (id TEXT, ' + getFieldNames(dataTable).join(' TEXT, ') + ' TEXT, last_update NUMERIC) PRIMARY KEY id;';
  // Somehow run this
  var insertStatement = 'INSERT INTO TABLE "' + tableName + '" (id, ' + getFieldNames(dataTable).join(', ') + ', last_update) VALUES ({{row_number}}, {{' + getFieldNames(dataTable).join('}}, {{') + '}}, {{currentTime}});';

  // Run inserts
  for (var k = 0; k < rows; k++) {
    replaceObj[k].rowNumber = k;
    replaceObj[k].currentTime = currentTime;
  // Somehow run these
  }
};
