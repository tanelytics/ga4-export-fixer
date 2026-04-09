const { isExcluded } = require('../../documentation.js');
const tableAgentInstructions = require('./columns/tableAgentInstructions.json');

/**
 * Builds the GA4-specific table description sections for ga4_events_enhanced.
 * These are passed to buildTableDescription() which adds shared sections
 * (package attribution, config JSON dump).
 *
 * @param {Object} config - The merged configuration object.
 * @returns {string[]} Array of description section strings.
 */
const getTableDescriptionSections = (config) => {
    // Only use user-configured excludedColumns for filtering AI instructions.
    // defaultExcludedColumns refers to raw GA4 export columns excluded during extraction
    // (e.g. session_id is excluded from the raw export but exists as a derived column in the final table).
    const excludedColumns = config.excludedColumns || [];

    const excludedEvents = [
        ...(config.defaultExcludedEvents || []),
        ...(config.excludedEvents || []),
    ];

    const sections = [];

    // 1. Overview
    const overviewLines = [
        'GA4 Events Enhanced',
        '',
        'An enhanced version of the GA4 BigQuery export. Each row is one event.',
    ];
    if (config.timezone) {
        overviewLines.push(`Timezone: ${config.timezone}.`);
    }
    sections.push(overviewLines.join('\n'));

    // 2. Key Fields
    const keyFieldLines = tableAgentInstructions.keyFields
        .filter(kf => !isExcluded(kf.dependsOn, excludedColumns))
        .map(kf => `- ${kf.field}: ${kf.note}`);

    // Add promoted event params
    if (config.eventParamsToColumns && config.eventParamsToColumns.length > 0) {
        config.eventParamsToColumns.forEach(p => {
            const columnName = p.columnName || p.name;
            keyFieldLines.push(`- ${columnName}: Promoted event parameter '${p.name}'. Available as a top-level column for direct filtering.`);
        });
    }

    if (keyFieldLines.length > 0) {
        sections.push('KEY FIELDS:\n' + keyFieldLines.join('\n'));
    }

    // 3. Synonyms
    const synonymLines = tableAgentInstructions.synonyms
        .filter(s => !isExcluded(s.dependsOn, excludedColumns))
        .map(s => `- "${s.terms.join('" / "')}" → ${s.sql}`);

    if (synonymLines.length > 0) {
        sections.push('SYNONYMS:\n' + synonymLines.join('\n'));
    }

    // 4. Filtering and Grouping
    const guidanceLines = tableAgentInstructions.filteringGuidance
        .filter(g => !isExcluded(g.dependsOn, excludedColumns))
        .map(g => `- ${g.text}`);

    if (guidanceLines.length > 0) {
        sections.push('FILTERING AND GROUPING:\n' + guidanceLines.join('\n'));
    }

    // 5. Event Vocabulary
    const vocabParts = [];
    const autoEvents = tableAgentInstructions.eventVocabulary.autoCollectedAndEnhanced
        .filter(e => !excludedEvents.includes(e));
    if (autoEvents.length > 0) {
        vocabParts.push(`Auto-collected and enhanced measurement: ${autoEvents.join(', ')}`);
    }

    if (!isExcluded(['ecommerce'], excludedColumns)) {
        const ecomEvents = tableAgentInstructions.eventVocabulary.ecommerce
            .filter(e => !excludedEvents.includes(e));
        if (ecomEvents.length > 0) {
            vocabParts.push(`Ecommerce (recommended): ${ecomEvents.join(', ')}`);
        }
    }

    if (vocabParts.length > 0) {
        sections.push('COMMON EVENT NAMES:\n' + vocabParts.join('\n'));
    }

    // 6. Table Features
    const featureLines = [
        'Combines daily, intraday, and fresh exports; the best available version of each event is used.',
        'Incremental updates: non-final data is replaced with the latest available data on every run.',
        'Promotes key fields (e.g. page_location, session_id) to top-level columns for faster queries.',
        'Session-level fields: landing_page, user_id resolution, and configurable session parameters.',
    ];
    sections.push('TABLE FEATURES:\n' + featureLines.map(f => `- ${f}`).join('\n'));

    return sections;
};

module.exports = { getTableDescriptionSections };
