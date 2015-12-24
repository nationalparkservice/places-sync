module.exports = function (newJsonA, newJsonB, origJson) {
  var fields = {};
  var field;
  var conflicts = [];
  var currentField;
  var newValues = {};
  var compare = function (a, b) {
    return JSON.stringify(a) === JSON.stringify(b);
  };

  for (field in newJsonA) {
    fields[field] = fields[field] || {};
    fields[field].newJsonA = newJsonA[field];
  }
  for (field in newJsonB) {
    fields[field] = fields[field] || {};
    fields[field].newJsonB = newJsonB[field];
  }
  for (field in origJson) {
    fields[field] = fields[field] || {};
    fields[field].origJson = origJson[field];
  }
  for (field in fields) {
    currentField = fields[field];

    // If A and B are the same, then we know that's the new value, no need to check the original
    if (compare(currentField.newJsonA, currentField.newJsonB)) {
      newValues[field] = currentField.newJsonA;
    } else if (compare(currentField.newJsonA, currentField.origJson)) {
      // JsonA is the same as the orig, so we should use the value from JsonB
      newValues[field] = currentField.newJsonB;
    } else if (compare(currentField.newJsonB, currentField.origJson)) {
      // JsonB is the same as the orig, so we should use the value from JsonA
      newValues[field] = currentField.newJsonA;
    } else {
      // This would mean that A, B, and Orig are different
      conflicts.push(field);
      newValues[field] = currentField.newJsonA;
    }
  }
  return {
    newValues: newValues,
    conflicts: conflicts
  };
};
