module.exports = function (database, columns) {
  return function () {
    // Writes the changes to the original file
    // TODO: Should this return the updates/removes that it saved?
    return Promise.all([
      database.query.apply(this, createQueries('getUpdated', undefined, columns)),
      database.query.apply(this, createQueries('getRemoved', undefined, columns))
    ]).then(function (results) {
      return Promise.all([
        masterCache ? masterCache.modify.applyUpdates({
          updated: rowsToMaster(results[0], columns, sourceConfig.name, sourceConfig.connection.processName, false),
          removed: rowsToMaster(results[1], columns, sourceConfig.name, sourceConfig.connection.processName, true)
        }) : tools.dummyPromise(),
        writeToSource(results[0], results[1]),
        modifySource.refresh(results[0], results[1])
      ]).then(function () {
        var fn = masterCache ? masterCache.save : tools.dummyPromise;
        return fn().then(function () {
          return tools.dummyPromise({
            'updated': results[0],
            'removed': results[1]
          });
        });
      });
    });
  };
};
