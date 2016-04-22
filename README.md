# places-sync

A node app that synchronizes data from multiple sources to the Places database.

## Installation

- `git clone https://github.com/nationalparkservice/places-sync.git`
- `npm install`
- Add your sources (this guide will be built in the future)

# How Sync Works

* "Load" "MasterCache"
  * sqlite database (currently)
    * schema:
      * *key text,
      * foreign_key text,
      * *process text,
      * *#source text,
      * hash text,
      * last_updated,
      * data blob,
      * is_removed numeric
* "Load" sourceA
  * Loads sourceA into memory
* "Load" sourceB
  * Loads sourceb into memory
* Get Updates From A since B last updated
* Apply Updates From A to B
* Save Source B
* Write Successful updates to MasterCache
* Close Source A
* Close Source B

### The "[Load](https://github.com/nationalparkservice/places-sync/blob/7ed155b81564176e0df51d3bf1fc2ec2ca011354/src/sources/index.js#L10)" Function:
  * some sources (text files, geojson, json, etc) are loaded entirely into memory as a sqlite database
  * other sources (that are queryable) are loaded "as is"

### The "[Get Updates](https://github.com/nationalparkservice/places-sync/blob/7ed155b81564176e0df51d3bf1fc2ec2ca011354/src/sources/helpers/createActions.js#L101)" Function
  * Ordered Tasks:
    1. Determines the last time source B was updated from source A
    2. Pulls all information into memory from source A that was created since the last sync

  * Unordered Tasks:
    1. Run Ordered Tasks
    2. Gets All keys from Source A to determine if anything from deleted since the last sync
    3. Gets all keys in the master cache, so we can determine what was previously syncronized from A to B\

  * Determine changes ([getUpdates.js](https://github.com/nationalparkservice/places-sync/blob/7ed155b81564176e0df51d3bf1fc2ec2ca011354/src/sources/helpers/getUpdates.js) and [getUdpates.sql](https://github.com/nationalparkservice/places-sync/blob/7ed155b81564176e0df51d3bf1fc2ec2ca011354/src/sources/helpers/getUpdates.sql])

### The "[Apply Updates](https://github.com/nationalparkservice/places-sync/blob/7ed155b81564176e0df51d3bf1fc2ec2ca011354/src/sources/helpers/createActions.js#L232)" Function
  * Accepts the object returned from "Get Updates"
    * It creates two "bins"
      1. A list of "removes" (anything marked as "removed")
      2. and a list of "inserts" (anything marked as "updated", "created", "missing")
    * It then adds the remove / updates to the source B object in memory

### The "[Save](https://github.com/nationalparkservice/places-sync/blob/7ed155b81564176e0df51d3bf1fc2ec2ca011354/src/sources/helpers/createActions.js#L164)" Function
  * Gets all updates to be run on the source
  * Gets all deletes to be run on the source
  * Pulls down the metadata object (this is used for extra information, such as a foreign key)

  * It writes / deletes the information from the source
  * Once the source has been successfully updated, it runs the "Apply Updates" and "Save" functions on the masterCache database

### The "[Close](https://github.com/nationalparkservice/places-sync/blob/7ed155b81564176e0df51d3bf1fc2ec2ca011354/src/sources/helpers/createActions.js#L199)" Function
  * Some databases and files require the connection to be closed, this step will close that connection
  * This step is required for all sources, as it will clear source object out of memory
