var Promise = require('bluebird');
var sources = require('places-sync-sources');
var tools = require('jm-tools');

module.exports = function (masterCache, sourceA, sourceB, options) {
  return new Promise(function (resolve, reject) {
    var setUpTasks = [{
      'name': 'masterCache',
      'description': 'Loads the source for the master sqlite database, gets info for it, and does not load data to cache',
      'task': sources,
      'params': [masterCache]
    }, {
      'name': 'sourceAConnection',
      'description': 'Loads the source for sourceA, gets info for it, and loads data to cache',
      'task': sources,
      'params': [sourceA, '{{masterCache}}']
    }, {
      'name': 'sourceBWithAColumns',
      'description': 'Adds the columns from sourceA connection to sourceB connection if it is required',
      'task': function (columnSource, columnDest) {
        if (options.copyColumns && columnSource &&  columnSource.get && columnSource.get.columns) {
          columnDest.columns = columnSource.get.columns();
        }
        return source;
      },
      'params': ['{{sourceAConnection}}', sourceB]
    }, {
      'name': 'sourceBConnection',
      'description': 'Loads the source for sourceB, gets info for it, and loads data to cache',
      'task': sources,
      'params': ['{{sourceBWithAColumns}}', '{{masterCache}}']
    }, {
      'name': 'updatesFromSourceA',
      'description': 'Gets an object containing the records that were updated and the records that were removed from sourceA since the last write to sourceB in the masterCache',
      'task': '{{sourceAConnection.get.updates}}',
      'params': ['{{sourceBConnection.get.name}}']
    }];

    var twoWayTasks = [{
      'name': 'updatedFromSourceB',
      'description': 'Gets an object containing the records that were updated and the records that were removed from sourceB since the last write to sourceA in the masterCache',
      'task': '{{sourceBConnection.get.updates}}',
      'params': [sourceA.name]
    }, {
      'name': 'applyUpdatesToSourceA',
      'description': 'Adds the updates and removes to the sourceA object',
      'task': '{{sourceAConnection.modify.applyUpdates}}',
      'params': ['{{updatedFromSourceB}}']
    }, {
      'name': 'saveSourceA',
      'description': 'Write data out to B and write it to the masterCache',
      'task': '{{sourceAConnection.save}}',
      'params': []
    }, {
      'name': 'closeSourceA',
      'description': 'Closes the Source and frees up memory',
      'task': '{{sourceAConnection.close}}',
      'params': []
    }];

    var saveSourceBTasks = [{
      'name': 'applyUpdatesToSourceB',
      'description': 'Adds the updates and removes to the b object',
      'task': '{{sourceBConnection.modify.applyUpdates}}',
      'params': ['{{updatesFromSourceA}}']
    }, {
      'name': 'saveSourceB',
      'description': 'Write data out to B and write it to the masterCache',
      'task': '{{sourceBConnection.save}}',
      'params': []
    }, {
      'name': 'closeSourceB',
      'description': 'Closes the Source and frees up memory',
      'task': '{{sourceBConnection.close}}',
      'params': []
    }];

    var taskList = setUpTasks;
    if (options && options.twoWay) {
      taskList = taskList.concat(twoWayTasks);
    }
    taskList = taskList.concat(saveSourceBTasks);

    tools.iterateTasks(taskList).then(function (results) {
      resolve(results);
    }).catch(function (e) {
      if (e === undefined) {
        e = new Error('undefined error');
      }
      reject(e);
    });
  });
};
