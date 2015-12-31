module.exports = function(task) {

  // Generate the master connection information

  // Check is master is locked, if so, wait or abort

  // Read the data from the sources
  // Returns a data object
    // Read Source A
    // Read Source B

  // Get a list of records that changed relative to the master
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


