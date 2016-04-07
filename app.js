var USAGE = 'USAGE: Create a process in the "./processes" directory\n';
USAGE += 'Specify that process with -p {processName} or --process {processname}\n';
// USAGE += 'This may be extended to take -s (source) and -d (destination), but not right now';

// Get the args using minimist
var argv = require('minimist')(process.argv.slice(2));
var tools = require('./src/tools');
var availableProcesses = tools.requireDirectory('./processes');
var selectedProcess = availableProcesses[argv.p || argv.process];
var runSync = require('./src/runSync');

if (!selectedProcess) {
  // Tell the user to choose a process
  console.error('╔══════════════════════════════════════════════');
  console.error('║' + USAGE.split('\n').join('\n║'));
  console.error('║ Valid processes are:');
  for (var processName in availableProcesses) {
    console.log('║ \t' + processName);
  }
  console.error('╔══════════════════════════════════════════════');
} else {
  runSync(selectedProcess).then(function (r) {
    console.log(JSON.stringify(r, null, 2));
  }).catch(function (e) {
    throw Array.isArray(e) ? e[e.length - 1] : e;
  });
}
