var atob = require('atob');
var btoa = require('btoa');
var lzstring = require('lz-string');
lzstring.compressToB = btoa;
lzstring.decompressFromB = atob;

var copyObjectLayer = function (newObj, err) {
  var res;
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

var stringify = function (j) {
  var newObj, err;
  if ((j.substr(0, 1) === '{' && j.substr(-1, 1) === '}') || (j.substr(0, 1) === '[' && j.substr(-1, 1) === ']')) {
    try {
      newObj = JSON.parse(j);
    } catch (e) {
      err = e;
    }
  } else {
    err = 'Does not match pattern of an object';
  }
  return {
    'err': err,
    'newObj': newObj
  };
};

var decode = function (b) {
  var newObj, err, tmp;
  var tests = [
    'compressToB',
    'compress',
    'compressToBase64',
    'compressToEncodedURIComponent'
    // 'compressToUTF16'
  ];
  for (var i = 0; i < tests.length; i++) {
    try {
      newObj = lzstring['de' + tests[i].replace('To', 'From')](b);
      console.log('vvvvv');
      console.log('b', b);
      console.log('newObj', newObj);
      console.log(tests[i], lzstring[tests[i]](newObj));
      console.log(b, '===', lzstring[tests[i]](newObj), ':', b === lzstring[tests[i]](newObj));
      console.log('^^^^^');
      if (b === lzstring[tests[i]](newObj)) {
        err = undefined;
        console.log('we got it!', b, '===', newObj);
      } else {
        newObj = undefined;
        err = 'Cannot decode string';
      }
    } catch (e) {
      console.log('error', e);
      err = e;
    }

    if (!err) {
      break;
    }
  }
  tmp = {
    'newObj': newObj,
    'err': err
  };
  if (!err) {
    return stringify(newObj);
  } else {
    return tmp;
  }
};

module.exports = function (j) {
  var res = {};
  var parsers;
  var tmp;
  if (typeof j === 'object') {
    res.newObj = j;
  } else if (typeof j === 'string') {
    if (j.length) {
      // We have a string, let's try to parse it!
      parsers = [
        stringify,
        decode
      ];

      for (var i = 0; i < parsers.length; i++) {
        tmp = parsers[i](j);
        if (!tmp.err) {
          res = tmp;
          break;
        }
      }
    } else {
      res.newObj = {};
    }
  } else {
    res.err = 'Value type ' + typeof j + ' not supported';
  }

  return copyObjectLayer(res.newObj, res.err);
};
