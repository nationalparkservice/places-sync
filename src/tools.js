var tools = module.exports = {
  arrayify: function (value) {
    return Array.isArray(value) ? value : [value];
  },
  readOutput: function (output) {
    return JSON.stringify(tools.arrayify(output).map(function (o) {
      if (o.toString().substr(0, 5) === 'Error') {
        return o.toString + '\\n' + o.stack;
      } else {
        return JSON.stringify(o, null, 2);
      }
    }), null, 2).replace(/\\n/g, '\n');
  },
  addTitles: function (titles, data) {
    var returnValue = {};
    titles.forEach(function (title, index) {
      returnValue[title] = data[index];
    });
    return returnValue;
  },
  getType: function (value, maxType) {
    var type = 'text';
    value = value.toString();
    if (!(isNaN(value) || value.replace(/ /g, '').length < 1) && maxType !== 'text') {
      type = 'float';
      if (parseFloat(value, 10) === parseInt(value, 10) && maxType !== 'float') {
        type = 'integer';
      }
    }
    return type;
  }
};
