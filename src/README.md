To create a database:

```
  var createDatabase = require('./createDatabase');
  var db = new createDatabase();
  db.addData({'data': 'columnA,columnB\n1,2\n3,4\n5,6'}, callback);
```
In addData, you must pass a source.

addData Imports the data from an object containing the following information: (name and data are the only required field)
   {name: (name for source) data: (see below), format: CSV, JSON, GEOJSON, columns: [{name: 'column', type: 'text'}], extractionType: FILE, URL, ARCGIS}

the data field can be either:
  1: A string that is CSV, JSON, or GEOJSON
  2: A string that uses a designator and a type to load a file or url or a custom source
    Examples: file:///test.csv, http://github.com

Example:
```
{
  columns: [{
    name: 'COLUMN NAME',
    type: 'SQLITE FORMAT' //text, float, integer
  }],
  data: String of (csv, json, geojson), also can be arcgis layer URL, file, or url (arcgis:https://, file:///, http://)
  description: 'SOURCE DESCRIPTION',
  editInfo: {
    dateEdited: 'COLUMN NAME' // field with last edited information
  }
  extractionType: 'arcgis', // arcgis, file, and url are currently supported
  format: 'geojson', // csv, json, geojson (arcgis is geojson)
  lastEditDate: 1453410311116, // UTC Time Stamp
  name: 'Datasource Name',
  primaryKey: 'COLUMN NAME', //Specify the column to the primary key, default is the first column 
}
```

You can then query for all the data in a hashed form
`source.getHashedDate(fromDateUTCTimestamp, callback);`
or for data with a specific primary key
`source.getRow(ValueOfPrimaryLey, callback);`
getRow will only return one record

`source.getDataWhere(whereObject, callback);`
getDataWhere allows you to specify a where object using syntax from sequelizejs

`source.getData(fromDate, callback);`
getData will get all data since a timestamp, if no timestamp is specified, it will just get all data

If a `callback` is omitted in any of these functions, a Bluebird promise will be returned instead.
