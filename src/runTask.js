module.exports = function(task) {

  // Generate the master connection information

  // Check is master is locked, if so, wait or abort

  // Read the data from the sources
  // Returns a data object
  // Read Source A
  // Read Source B



  // Get a list of records that changed relative to the master
  // If we are only looking at records since the last merge (as opposed to all records), we will miss deleted records
  // There are a few ways to solve this
  // Do a query on just the ids in A and B and compare them to the ids in Master
  // Instead of deleting these records from the table, mark them as removed and include a new update time
  // Duplicate Primary keys will cause a "conflict"
  // Returns an object such as {'id': 'diff'} if source is different from master
  // Returns an object such as {'id': 'new'} if source is not in master
  // Returns an object such as {'id': 'removed'} if source is not in master
  // Compare Source A
  // Compare Source B

  // Compare A and B new fields
  // if a.id === b.id then insert to C
  // if a.id !== b.id then merge
  // if a.id and not b.id insert to c and a
  // if not a.id and b.id insert to c and b
  // if c.id and not a.id then delete from b
  // if c.id and not b.id then delete from b

  // Attempt to merge the diff sources, and return conflicts
  // Merge A, B, and Master (C)

  // Run functions on A, B, and Master
  // Lock Master so no compares can be run
  // Run Deletes and Creates on A
  // Run Deletes and Creates on B
  // Run Deletes and Creates on Master (master 'deletes' are just a column the denotes the record has been removed)
  // Unlock Master

  // Done
};

var readSource(source) {
  // Open Database Connection
  // Run the SELECT Query
  // Format the data
  // This includes creating a hash for comparing
  // Return the formatted Data
};

var compareWithMaster(source, master) {
  // FULL OUTER JOIN source and master on id
  // if match only in source, then add to output with new
  // if match only in master, then add to output with removed
  // if source hash !== master hash, then add to output with diff
  // return {'id': 'changeType'}
};

var compareFields = function(sourceA, sourceB, diffsA, diffsB) {
  // if a.id === b.id then insert to C
  // if a.id !== b.id then merge (merge function)
  // if a.id and not b.id insert to c and a (merge function)
  // if not a.id and b.id insert to c and b (merge function)
  // if c.id and not a.id then delete from b
  // if c.id and not b.id then delete from a

  // Merge together the diff lists
  var record;
  var diffs = {};
  for (record in diffA) {
    diffs[record] = diffs[record] || {};
    diffs[record].a = diffA.record
  }
  for (record in diffB) {
    diffs[record] = diffs[record] || {};
    diffs[record].b = diffB.record
  }

  for (record in diffs) {
    if (diffs[record].a === 'new' || diffs[record].a === 'diff' || diffs[record].b === 'new' || diffs[record].b === 'diff') {
      if (sourceA[record] && sourceA[record].hash === sourceB[record] && sourceB[record].hash) {
        // Insert to Master
      } else {
        // Covers
        // if a.id !== b.id then merge
        // if a.id and not b.id insert to c and a
        // if not a.id and b.id insert to c and b
        // Add to merge list (or merge here?)
      }
    } else if (diffs[record].a === 'removed' || diffs[record].b === 'removed') {
      if (diffs[record].a === 'removed') {
        // Delete from b and source
      } else if (diffs[record].b === 'removed') {
        // Delete from a nd source
      } else {
        // Impossible condition, throw error
      }
    } else {
      // Impossible condition, throw error
    }
  }
};
