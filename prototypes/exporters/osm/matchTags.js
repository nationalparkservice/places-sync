var datawrap = require('datawrap');
var runList = datawrap.runList;
var Bluebird = datawrap.Bluebird;
var createWhereClause = require('../../../src/createWhereClause');
var tools = require('../../../src/tools');

module.exports = function (gisSource, database, where) {
  return new Bluebird(function (fulfill, reject) {
    // Allow the source or just its name to be passed in
    if (typeof gisSource === 'string') {
      gisSource = database.sources[gisSource];
    }

    var taskList = [{
      'name': 'Load Tag Source Data',
      'description': 'Loads the csv files into the database for tag matching',
      'task': loadTagData,
      'params': [database]
    }, {
      'name': 'Get Datatype',
      'description': 'Loads the datatype information from the translation document',
      'task': getDatatype,
      'params': [gisSource._source().placesDatatype, '{{Load Tag Source Data}}']
    }, {
      'name': 'Get Translations',
      'description': 'Creates a list of what fields and values we need to translate to Places Tags',
      'task': getTranslations,
      'params': [gisSource, '{{Load Tag Source Data}}', '{{Get Datatype}}']
    }, {
      'name': 'Convert Translations',
      'description': 'Converts the translations into CASE WHEN statements for a database query',
      'task': translationsToQuery,
      'params': [gisSource, '{{Get Translations}}', '{{Get Datatype}}', where]
    }, {
      'name': 'Run Translation Query',
      'description': 'Runs the query built by the Convert Translations function',
      'task': database._runQuery,
      'params': ['{{Convert Translations}}']
    }, {
      'name': 'Remove Nulls',
      'description': 'Removes null values from the objects returned in the query',
      'task': removeNulls,
      'params': ['{{Run Translation Query}}']
    }];

    runList(taskList).then(function (results) {
      fulfill(results[results.length - 1]);
    }).catch(function (e) {
      reject(e[e.length - 1]);
    });
  });
};

var loadTagData = function (database) {
  return new Bluebird(function (fulfill, reject) {
    var asyncTaskList = [
      database.addData(createTagSourceTemplate('datatypes', 'datatypes.csv', ['Name'])),
      database.addData(createTagSourceTemplate('presets', 'presets.csv', ['superclass', 'class', 'type'])),
      database.addData(createTagSourceTemplate('fields', 'fields.csv', 'GIS Field Name')),
      database.addData(createTagSourceTemplate('values', 'values.csv', ['Translator', 'GIS Field Name', 'GIS Field Value']))
    ];
    Bluebird.all(asyncTaskList).then(function (results) {
      var returnObj = {};
      results.forEach(function (result) {
        if (result && result._source) {
          returnObj[result._source().tagSource] = result;
        }
      });
      fulfill(returnObj);
    }).catch(reject);
  });
};

var getDatatype = function (sourceType, tagSources) {
  return new Bluebird(function (fulfill, reject) {
    tagSources.datatypes.getRow(sourceType).then(function (datatype) {
      // Add a places types column to the data type to make it easy to query the presets table
      datatype['Place Types'] = {};
      var geometryTypes = parseJson(datatype['Geometry Types'], []);
      geometryTypes.forEach(function (type) {
        datatype['Place Types'][type.toLowerCase().replace('polyline', 'line')] = 'x';
      });

      fulfill(datatype);
    }).catch(reject);
  });
};

var createTagSourceTemplate = function (name, path, primaryKey) {
  return {
    'name': 'tag_source_' + name,
    'tagSource': name,
    'data': __dirname + '/translations/' + path,
    'format': 'csv',
    'extractionType': 'file',
    'primaryKey': primaryKey
  };
};

var parseJson = function (value, returnOnError) {
  var returnValue;
  if (typeof value === 'string') {
    try {
      returnValue = JSON.parse(value);
    } catch (e) {
      returnValue = returnOnError;
    }
  }
  return returnValue || returnOnError;
};

var getTranslations = function (gisSource, tagSources, datatype) {
  return new Bluebird(function (fulfill, reject) {
    Bluebird.all([
      tagSources.fields.getDataWhere({
        'Translator': [gisSource._source().placesDatatype, 'generic'],
        'Add To Places': 'TRUE'
      }),
      tagSources.values.getDataWhere({
        'Translator': [gisSource._source().placesDatatype, 'generic']
      }),
      tagSources.presets.getDataWhere(datatype['Place Types'])
    ]).then(function (resultData) {
      var gisSourceColumns = tools.simplifyArray(gisSource._source().columns);
      var fieldData = resultData[0];
      var valueData = resultData[1];
      var presetData = resultData[2];
      var translations = [];

      var fieldTranslations = getFieldTranslations(gisSourceColumns, fieldData);
      var valueTranslations = getValueTranslations(gisSourceColumns, valueData, presetData);
      translations = translations.concat(fieldTranslations, valueTranslations);
      fulfill(translations);
    }).catch(reject);
  });
};

var getFieldTranslations = function (gisSourceColumns, fieldData) {
  var fieldTranslations = [];

  // Go through each field and see if we need to add it
  fieldData.forEach(function (row) {
    var rank = 1;
    var sourceColumnsIndex = gisSourceColumns.indexOf(row['GIS Field Name']);
    var parsedAltValues, i;

    // If we don't have this column, we can check to see if we have any of the alternate columns for this
    if (sourceColumnsIndex === -1 && row['Alternate GIS Names'].length > 2) {
      rank = 2;
      parsedAltValues = parseJson(row['Alternate GIS Names'], []);

      // Find the first column that matches anything in our sources (if any)
      for (i = 0; i < parsedAltValues.length; i++) {
        if (gisSourceColumns.indexOf(parsedAltValues[i]) > -1) {
          sourceColumnsIndex = gisSourceColumns.indexOf(parsedAltValues[i]);
          break;
        }
      }
    }

    // If anything matches, let's add it to the field translations
    if (sourceColumnsIndex > -1) {
      fieldTranslations.push({
        'sourceColumn': gisSourceColumns[sourceColumnsIndex],
        'destColumn': row['Places Tag Name'],
        'rank': rank
      });
    }
  });
  return fieldTranslations;
};

var getValueTranslations = function (gisSourceColumns, valueData, presetData) {
  var valueTranslations = [];

  var addTranslations = function (value) {
    var alt, then;
    var valueTranslation = [];

    if (value['GIS Field Value'] === '*' || value['Alternate GIS Values'] === '*' || value['Places Tags'] === '*') {
      // Match to the presets data
      presetData.forEach(function (preset) {
        var subValue = {
          'GIS Field Name': value['GIS Field Name'],
          'GIS Field Value': preset.type,
          'Alternate GIS Values': preset.altNames,
          'Places Tags': preset.tags
        };
        addTranslations(subValue).forEach(function (translation) {
          valueTranslation.push(translation);
        });
      });
    } else {
      alt = parseJson(value['Alternate GIS Values'], []);
      alt.unshift(value['GIS Field Value']);
      then = parseJson(value['Places Tags'], {});

      // When rank is the order in which the value can be found in the alt names array
      // This is used to determine how soon something should match in case of duplicates
      // This should be extended to use matchScore that iD uses
      // the match scores in iD are set here:
      // https://github.com/nationalparkservice/places-data/blob/8ea9ed2bc115ca53db67719532b5fd06aeec192d/tools/buildFromCSV.js#L148
      // TODO: If matchScore becomes a column in the spreadsheet, we can use it in this tool as well
      for (var tag in then) {
        alt.forEach(function (gisValue) {
          valueTranslation.push({
            'sourceColumn': value['GIS Field Name'],
            'destColumn': tag,
            'rank': 0,
            'when': gisValue,
            'then': then[tag],
            'whenRank': valueTranslation.length
          });
        });
      }
    }
    return valueTranslation;
  };

  // Go through each column in our source and see if we have a value translation for it
  gisSourceColumns.forEach(function (gisSourceColumn) {
    // Look at each translation value
    valueData.forEach(function (value) {
      // If we have the column for one of the translations, add it to the translations array
      if (value['GIS Field Name'] === gisSourceColumn) {
        valueTranslations = valueTranslations.concat(addTranslations(value));
      }
    });
  });

  return valueTranslations;
};
var addParameter = function (param) {
  // These queries tend to have too many parameters for parameterized queries
  // http://www.sqlite.org/limits.html#max_variable_number (Defaults to 999)
  // Even in this fairly safe environment (loading from predefined files) it is
  // bad practice to load strings right into SQL. So our other option is to convert
  // all input to binary objects: http://www.sqlite.org/lang_expr.html
  // BLOB literals are string literals containing hexadecimal data and preceded by a single "x" or "X" character. Example: X'53514C697465'
  if (param === null || param === undefined) {
    return null;
  }
  var returnValue = "CAST(x'";
  for (var i = 0; i < param.length; i++) {
    returnValue += param.charCodeAt(i).toString(16);
  }
  return returnValue + "' AS TEXT)";
// return '"' + param + '"';
};

var translationsToQuery = tools.syncPromise(function (gisSource, translations, datatype, where) {
  // Now that we have all of the translations, we group them by destination field
  // this is because we only want to display one of each field
  // These are ranked by what's most important to us
  // Rank 0: If there is a value translation for a field, we want to use that
  // Rank 1: if there is no value translation for a field, we just just rename the field to one we want
  // Rank 2: if none of our primary names exist for a field rename, then we look at alternate field values
  var queryFields = tools.groupAndRank(translations, 'destColumn', 'rank', ['sourceColumn', 'when', 'then', 'whenRank']);
  var sourceColumns = tools.simplifyArray(gisSource._source().columns);
  var defaultTags = parseJson(datatype['Default Tags'], {});
  var queryString;

  var getCases = function (sourceField, caseWhen, caseThen) {
    // We try to do all our matching in lower case, but this only works on strings
    caseWhen = caseWhen && caseWhen.toLowerCase ? caseWhen.toLowerCase() : caseWhen;
    caseWhen = caseWhen ? '= ' + addParameter(caseWhen) : 'IS NOT NULL';
    caseThen = caseThen ? addParameter(caseThen) : '"' + sourceField + '"';
    return 'WHEN LOWER("' + sourceField + '") ' + caseWhen + ' THEN ' + caseThen;
  };

  // Add the default fields to the queryFields
  /* sourceColumn: [ 'ASSETID' ],
    destColumn: 'nps:asset_id',
    rank: 1,
    when: [ undefined ],
    then: [ undefined ],
    whenRank: [ undefined ] } */
  var destColumns = queryFields.map(function (queryField) {
    return queryField.destColumn;
  });
  for (var defaultTag in defaultTags) {
    if (destColumns.indexOf(defaultTag) === -1) {
      // Add the default field in
      queryFields.push({
        'sourceColumn': [sourceColumns[0]],
        'destColumn': defaultTag,
        'rank': 1,
        'then': [defaultTags[defaultTag]],
        'when': [],
        'whenRank': []
      });
    }
  }

  var columnsWithCases = queryFields.map(function (queryField) {
    // Define the ELSE string, usually null, but sometimes we have default values for things (as defined on the Translators tab)
    var caseElse = defaultTags[queryField.destColumn] ? addParameter(defaultTags[queryField.destColumn]) : 'NULL';

    // Build the CASE WHEN x THEN y statements
    var caseStatementsWithRank = queryField.sourceColumn.map(function (field, index) {
      return [queryField.whenRank[index], getCases(field, tools.arrayify(queryField.when)[index], tools.arrayify(queryField.then)[index])];
    });

    // We rank the when statements by how deep into the alt array they are, so that we can first match on "GIS Value Name" for all columns
    // Then we dig into the alt array, giving better matching treatment based on where on the array the match is located
    var caseStatements = caseStatementsWithRank.sort(function (a, b) {
      return a[0] - b[0];
    }).map(function (a) {
      return a[1];
    });

    // Remove any duplicate case statements
    caseStatements = tools.dedupe(caseStatements);

    // Join the case statements together into a string
    var caseString = caseStatements.join('\n    ');

    return '  CASE ' + caseString + '\n    ELSE ' + caseElse + '\n    END AS "' + queryField.destColumn + '"';
  });

  // Create the where clause
  var whereClauseParts = createWhereClause(where, sourceColumns);
  for (var value in whereClauseParts[1]) {
    whereClauseParts[1][value] = addParameter(whereClauseParts[1][value]);
  }
  var whereClause = where ? ' WHERE ' + datawrap.fandlebars(whereClauseParts[0], whereClauseParts[1]) : '';

  var source = gisSource._source && gisSource._source() || {};

  var primaryKeyColumns = tools.arrayify(source.primaryKey).map(function (col) {
    return '"' + col + '"';
  }).join(' || ') + ' AS "_primary_key",\n';
  var editDateField = source.editingInfo && source.editingInfo.dateEdited;
  var lastEditColumn = '"' + editDateField + '" AS "_last_edit",\n';

  queryString = 'SELECT\n';
  queryString += sourceColumns.indexOf('geometry') > -1 ? '"geometry",\n' : '';
  queryString += source.primaryKey ? primaryKeyColumns : '';
  queryString += editDateField ? lastEditColumn : '';
  queryString += columnsWithCases.join(',\n');
  queryString += '\nFROM\n';
  queryString += '"' + gisSource.name + '"\n';
  queryString += whereClause;
  queryString += ';';

  return queryString;
});

var removeNulls = tools.syncPromise(function (queryData) {
  return queryData[0].map(function (row) {
    return tools.denullify(row);
  });
});
