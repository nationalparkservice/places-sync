module.exports = function (sourceA, sourceB, originalSource) {
  var fields = {};
  var field;
  var conflicts = [];
  var currentField;
  var newValues = {};
  var compare = function (a, b) {
    return JSON.stringify(a) === JSON.stringify(b);
  };

  for (field in sourceA) {
    fields[field] = fields[field] || {};
    fields[field].sourceA = sourceA[field];
  }
  for (field in sourceB) {
    fields[field] = fields[field] || {};
    fields[field].sourceB = sourceB[field];
  }
  for (field in originalSource) {
    fields[field] = fields[field] || {};
    fields[field].originalSource = originalSource[field];
  }
  for (field in fields) {
    currentField = fields[field];

    // If A and B are the same, then we know that's the new value, no need to check the original
    if (compare(currentField.sourceA, currentField.sourceB)) {
      newValues[field] = currentField.sourceA;
    } else if (compare(currentField.sourceA, currentField.originalSource)) {
      // JsonA is the same as the orig, so we should use the value from JsonB
      newValues[field] = currentField.sourceB;
    } else if (compare(currentField.sourceB, currentField.originalSource)) {
      // JsonB is the same as the orig, so we should use the value from JsonA
      newValues[field] = currentField.sourceA;
    } else {
      // This would mean that A, B, and Orig are different
      conflicts.push(field);
      newValues[field] = currentField.sourceA;
    }
  }
  return {
    newValues: newValues,
    conflicts: conflicts
  };
};
