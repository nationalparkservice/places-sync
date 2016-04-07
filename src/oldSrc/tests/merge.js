var csvDb = require('../csvDb');

// Load CSV A
// Add the source A to the merging database
  // When initialized, it will work like this:
    // Merge Source A to master
    // Merge Source A to B -- only required for a one way sync, otherwise we'd need to initialize source B
// Add the source B to the merging database
  // TODO, similar to source A, but only required for 2 way sync
// Load CSV A updates
  // If date_created, drop all records with ids in the new group
  // If no date_created, drop all records and re-import
  // If no removed field, do a query of all ids and compare them to current ids, if any are not present in the new source, delete them from our cache
// Load CSV B updates
  // TODO, this is not required for a one way sync
// Run merge tool, get list of (CREATE, UPDATES, CONFLICT)
// Deal with conflicts
  // TODO
  // pull source info from A and B for conflicts
  // Run the compare tool to determine what the new field is
  // If conflicts still occur, write back to the conflict field on both A and B, and do not send either creates or updates for that ID
// RUN CREATES on all DBs
  // Only source B for one way
// RUN UPDATES on all DBs
  // Only source B for one way
