/**
 * Runs all unit test suites and prints a compact summary.
 *
 * Usage:
 *   npm run test:summary
 */

const { execSync } = require('child_process');

const suites = [
    { name: 'ga4EventsEnhanced', cmd: 'node tests/ga4EventsEnhanced.test.js' },
    { name: 'assertions', cmd: 'node tests/assertions.test.js' },
    { name: 'mergeSQLConfigurations', cmd: 'node tests/mergeSQLConfigurations.test.js' },
    { name: 'preOperations', cmd: 'node tests/preOperations.test.js' },
    { name: 'documentation', cmd: 'node tests/documentation.test.js' },
    { name: 'inputValidation', cmd: 'node tests/inputValidation.test.js' },
    { name: 'createTable', cmd: 'node tests/createTable.test.js' },
    { name: 'queryBuilder', cmd: 'node tests/queryBuilder.test.js' },
    { name: 'customSteps', cmd: 'node tests/customSteps.test.js' },
];

let totalPassed = 0;
let totalFailed = 0;
const results = [];

for (const suite of suites) {
    try {
        const output = execSync(suite.cmd, { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] });

        // Extract counts from output — match "Passed: N" and "Failed: N" patterns
        const passedMatch = output.match(/Passed:\s*(\d+)/i) || output.match(/(\d+)\s*passed/i);
        const failedMatch = output.match(/Failed:\s*(\d+)/i) || output.match(/(\d+)\s*failed/i);
        const p = passedMatch ? parseInt(passedMatch[1], 10) : 0;
        const f = failedMatch ? parseInt(failedMatch[1], 10) : 0;

        totalPassed += p;
        totalFailed += f;
        results.push({ name: suite.name, passed: p, failed: f, error: null });
    } catch (err) {
        const output = (err.stdout || '') + (err.stderr || '');
        const passedMatch = output.match(/Passed:\s*(\d+)/i) || output.match(/(\d+)\s*passed/i);
        const failedMatch = output.match(/Failed:\s*(\d+)/i) || output.match(/(\d+)\s*failed/i);
        const p = passedMatch ? parseInt(passedMatch[1], 10) : 0;
        const f = failedMatch ? parseInt(failedMatch[1], 10) : 0;

        totalPassed += p;
        totalFailed += f;
        results.push({ name: suite.name, passed: p, failed: f, error: output });
    }
}

// Print summary
console.log('\n  Suite                     Passed  Failed');
console.log('  ' + '-'.repeat(44));
for (const r of results) {
    const status = r.failed > 0 ? 'FAIL' : ' OK ';
    console.log(`  [${status}] ${r.name.padEnd(22)} ${String(r.passed).padStart(4)}  ${String(r.failed).padStart(4)}`);
}
console.log('  ' + '-'.repeat(44));
console.log(`  Total${' '.repeat(24)} ${String(totalPassed).padStart(4)}  ${String(totalFailed).padStart(4)}`);

if (totalFailed > 0) {
    console.log('\nFailed suites:\n');
    for (const r of results.filter(r => r.failed > 0)) {
        console.log(`--- ${r.name} ---`);
        console.log(r.error || '(no output captured)');
        console.log('');
    }
    process.exit(1);
} else {
    console.log('\n  All tests passed.\n');
}
