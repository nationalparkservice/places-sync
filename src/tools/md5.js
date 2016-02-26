var crypto = require('crypto');
module.exports = function (input) {
  return crypto.createHash('md5')
    .update(input)
    .digest('hex');
};
