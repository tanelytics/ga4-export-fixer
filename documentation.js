const columnDescriptions = require('./columns/columnDescriptions.json');
const columnLineage = require('./columns/columnLineage.json');
const columnTypicalUse = require('./columns/columnTypicalUse.json');
const tableAgentInstructions = require('./columns/tableAgentInstructions.json');
const constants = require('./constants');

/**
 * Composes a multi-section column description string from individual sections.
 * Sections with null/undefined/empty values are omitted.
 * Sections are separated by line breaks for readability.
 *
 * @param {Object} sections - { base, lineage, typicalUse, config }
 * @returns {string} Composed description
 */
const composeDescription = (sections) => {
    const parts = [];

    if (sections.base) {
        parts.push(sections.base);
    }

    if (sections.lineage) {
        parts.push(`Lineage: ${sections.lineage}`);
    }

    if (sections.typicalUse) {
        parts.push(`Typical use: ${sections.typicalUse}`);
    }

    if (sections.config) {
        parts.push(`Config: ${sections.config}`);
    }

    return parts.join('\n\n');
};

/**
 * Returns a formatted lineage text string for a column, or null if no lineage data exists.
 *
 * @param {string} columnName - The column name to look up.
 * @returns {string|null} Formatted lineage string, e.g. "Derived -- Concatenation of ..."
 */
const getLineageText = (columnName) => {
    const entry = columnLineage[columnName];
    if (!entry) return null;

    const sourceLabels = {
        'ga4_export': 'Standard GA4 export field',
        'ga4_export_modified': 'GA4 export field (modified)',
        'derived': 'Derived',
    };

    const label = sourceLabels[entry.source] || entry.source;
    return entry.note ? `${label} -- ${entry.note}` : label;
};

/**
 * Builds a map of config-specific notes for columns based on the provided configuration.
 * Extracts the configuration-dependent description suffixes into a { columnName: "note" } map.
 *
 * @param {Object} config - The merged configuration object.
 * @returns {Object} Map of column names to config note strings.
 */
const buildConfigNotes = (config) => {
    const notes = {};

    if (!config) return notes;

    const append = (key, text) => {
        notes[key] = notes[key] ? `${notes[key]}. ${text}` : text;
    };

    // timezone
    if (config.timezone) {
        append('event_datetime', `Timezone: ${config.timezone}`);
    }

    // customTimestampParam
    if (config.customTimestampParam) {
        append('event_datetime', `Custom timestamp parameter: '${config.customTimestampParam}'`);
        append('event_custom_timestamp', `Source parameter: '${config.customTimestampParam}'`);
    }

    // data_is_final
    if (config.dataIsFinal) {
        const method = config.dataIsFinal.detectionMethod;
        if (method === 'DAY_THRESHOLD') {
            append('data_is_final', `Detection method: DAY_THRESHOLD (${config.dataIsFinal.dayThreshold} days)`);
        } else {
            append('data_is_final', `Detection method: EXPORT_TYPE`);
        }
    }

    // excludedEvents
    if (config.excludedEvents && config.excludedEvents.length > 0) {
        append('event_name', `Excluded events: ${config.excludedEvents.join(', ')}`);
    }

    // excludedEventParams
    if (config.excludedEventParams && config.excludedEventParams.length > 0) {
        append('event_params', `Excluded parameters: ${config.excludedEventParams.join(', ')}`);
    }

    // sessionParams
    if (config.sessionParams && config.sessionParams.length > 0) {
        append('session_params', `Configured parameters: ${config.sessionParams.join(', ')}`);
    }

    // includedExportTypes
    if (config.includedExportTypes) {
        const types = Object.entries(config.includedExportTypes)
            .filter(([, enabled]) => enabled)
            .map(([type]) => type);
        if (types.length > 0) {
            append('export_type', `Included export types: ${types.join(', ')}`);
        }
    }

    return notes;
};

/**
 * Returns a deep copy of the default column descriptions, enriched with
 * lineage, typical use, and configuration-specific sections composed into
 * multi-section descriptions.
 *
 * @param {Object} config - The merged configuration object from ga4EventsEnhanced.
 * @returns {Object} Column descriptions object in Dataform ITableConfig columns format.
 */
const getColumnDescriptions = (config) => {
    const descriptions = JSON.parse(JSON.stringify(columnDescriptions));

    const configNotes = buildConfigNotes(config);

    // Compose multi-section descriptions for each top-level column
    for (const key of Object.keys(descriptions)) {
        const isStruct = typeof descriptions[key] === 'object' && descriptions[key].description;
        const baseDesc = isStruct ? descriptions[key].description : (typeof descriptions[key] === 'string' ? descriptions[key] : null);

        if (!baseDesc) continue;

        const composed = composeDescription({
            base: baseDesc,
            lineage: getLineageText(key),
            typicalUse: columnTypicalUse[key] || null,
            config: configNotes[key] || null,
        });

        if (isStruct) {
            descriptions[key].description = composed;
        } else {
            descriptions[key] = composed;
        }
    }

    // Add descriptions for dynamically promoted event parameter columns
    if (config && config.eventParamsToColumns && config.eventParamsToColumns.length > 0) {
        config.eventParamsToColumns.forEach(p => {
            const columnName = p.columnName || p.name;
            const type = p.type ? ` (${p.type})` : ' (any data type)';
            descriptions[columnName] = composeDescription({
                base: `Promoted from event parameter '${p.name}'${type}`,
                lineage: `Derived -- Promoted from the event_params array`,
                typicalUse: 'Promoted event parameter available as a top-level column for direct filtering and aggregation',
                config: null,
            });
        });
    }

    return descriptions;
};

/**
 * Checks whether a column (or its parent struct) is excluded by the config.
 *
 * @param {string[]} dependsOn - Column names this entry depends on.
 * @param {string[]} excludedColumns - Combined excluded columns from config.
 * @returns {boolean} True if ALL dependsOn columns are excluded.
 */
const isExcluded = (dependsOn, excludedColumns) => {
    if (!dependsOn || dependsOn.length === 0) return false;
    return dependsOn.every(col => excludedColumns.includes(col));
};

/**
 * Composes the full table description for ga4_events_enhanced, including
 * AI agent instructions (key fields, synonyms, filtering guidance, event vocabulary)
 * and the existing table features and config JSON dump.
 *
 * @param {Object} config - The merged configuration object.
 * @returns {string} The composed table description.
 */
const getTableDescription = (config) => {
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

    // 7. Package Attribution
    sections.push(`${constants.TABLE_DESCRIPTION_SUFFIX}\n${constants.TABLE_DESCRIPTION_DOCUMENTATION_LINK}`);

    // 8. Config JSON dump
    const configJson = JSON.stringify(
        Object.fromEntries(
            Object.entries(config).filter(([key]) => !key.startsWith('default') && key !== 'dataformTableConfig')
        ),
        null,
        2
    );
    sections.push(`The last full table refresh was done using this configuration:\n${configJson}`);

    return sections.join('\n\n');
};

module.exports = {
    columnDescriptions,
    getColumnDescriptions,
    getTableDescription,
    composeDescription,
    getLineageText,
    buildConfigNotes,
};
