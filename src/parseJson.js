var atob = require('atob');

module.exports = function (j) {
  var newObj = {};
  var res;
  var err;
  if (typeof j === 'string') {
    if ((j.substr(0, 1) === '{' && j.substr(-1, 1) === '}') || (j.substr(0, 1) === '[' && j.substr(-1, 1) === ']')) {
      // We probably have an object
      try {
        newObj = JSON.parse(j);
      } catch (e) {
        err = e;
      }
    } else if (j.length) {
      // It might be encoded
      try {
        newObj = atob(j);
      } catch (e) {
        err = e;
      }
      try {
        newObj = JSON.parse(newObj);
      } catch (e) {
        err = e;
      }
    } else {
      err = 'Empty values are not supported';
    }
  } else if (typeof j === 'object') {
    newObj = j;
  } else {
    err = 'Value type ' + typeof j + ' not supported';
  }

  if (!err && typeof newObj === 'object') {
    res = {};
    for (var i in newObj) {
      res[i] = newObj[i];
    }
  } else {
    err = err || 'Value cannot be made into an object';
    newObj = undefined;
  }
  return {
    'e': err,
    'r': res
  };
};
