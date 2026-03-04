/*
Updates the version of the ga4-export-fixer package in the README.md file.
*/
const fs = require('fs');
const pkg = require('../package.json');
let readme = fs.readFileSync('README.md', 'utf8');
readme = readme.replace(/"ga4-export-fixer": "[\d.]+"/g, `"ga4-export-fixer": "${pkg.version}"`);
fs.writeFileSync('README.md', readme);

console.log(`Updating README version to ${pkg.version}`);