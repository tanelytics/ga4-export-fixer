/*
Updates the README.md file:
- Generates a table of contents from markdown headings (between <!-- TOC --> markers)
- Updates the package version references (skipped for dev versions)
*/
const fs = require('fs');
const pkg = require('../package.json');

let readme = fs.readFileSync('README.md', 'utf8');

// --- Table of contents ---

function generateToc(markdown) {
    const lines = markdown.split('\n');
    const tocEntries = [];

    let inCodeBlock = false;
    let pastTocMarker = false;
    for (const line of lines) {
        if (line.includes('<!-- /TOC -->')) pastTocMarker = true;
        if (line.trimStart().startsWith('```')) {
            inCodeBlock = !inCodeBlock;
            continue;
        }
        if (inCodeBlock) continue;

        if (!pastTocMarker) continue;

        const match = line.match(/^(#{2,3})\s+(.+?)[\r\n]*$/);
        if (!match) continue;

        const level = match[1].length;
        const text = match[2].trim();
        const slug = text
            .toLowerCase()
            .replace(/[^\w\s-]/g, '')
            .replace(/\s+/g, '-');
        const indent = '  '.repeat(level - 2);
        tocEntries.push(`${indent}- [${text}](#${slug})`);
    }

    return tocEntries.join('\n');
}

const tocStart = '<!-- TOC -->';
const tocEnd = '<!-- /TOC -->';
const tocPattern = new RegExp(`${tocStart}[\\s\\S]*?${tocEnd}`);

if (tocPattern.test(readme)) {
    const toc = generateToc(readme);
    readme = readme.replace(tocPattern, `${tocStart}\n${toc}\n${tocEnd}`);
    console.log('Updated table of contents.');
} else {
    console.log('No TOC markers found in README.md, skipping TOC generation.');
}

// --- Version update (production only) ---

if (pkg.version.includes('-')) {
    console.log(`Skipping version update for dev version ${pkg.version}`);
} else {
    readme = readme.replace(/"ga4-export-fixer": "[\d.]+"/g, `"ga4-export-fixer": "${pkg.version}"`);
    console.log(`Updated README version to ${pkg.version}`);
}

fs.writeFileSync('README.md', readme);