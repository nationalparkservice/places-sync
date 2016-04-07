var parseCsv = require('../formats/csv.js');
parseCsv({'data': 'a,b,c\n1,2,3\n5,6,7'}).then(console.log).catch(console.log);
