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
 * @param {Object} columnLineage - The lineage data object mapping column names to { source, note }.
 * @returns {string|null} Formatted lineage string, e.g. "Derived -- Concatenation of ..."
 */
const getLineageText = (columnName, columnLineage) => {
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
 * Returns a deep copy of the column descriptions, enriched with
 * lineage, typical use, and configuration-specific sections composed into
 * multi-section descriptions.
 *
 * @param {Object} config - The merged configuration object.
 * @param {Object} columnMetadata - Column metadata provided by the table module.
 * @param {Object} columnMetadata.descriptions - Column descriptions (Dataform ITableConfig columns format).
 * @param {Object} columnMetadata.lineage - Column lineage data mapping column names to { source, note }.
 * @param {Object} columnMetadata.typicalUse - Column typical use mapping column names to description strings.
 * @returns {Object} Column descriptions object in Dataform ITableConfig columns format.
 */
const getColumnDescriptions = (config, columnMetadata) => {
    const descriptions = JSON.parse(JSON.stringify(columnMetadata.descriptions));

    const configNotes = buildConfigNotes(config);

    // Compose multi-section descriptions for each top-level column
    for (const key of Object.keys(descriptions)) {
        const isStruct = typeof descriptions[key] === 'object' && descriptions[key].description;
        const baseDesc = isStruct ? descriptions[key].description : (typeof descriptions[key] === 'string' ? descriptions[key] : null);

        if (!baseDesc) continue;

        const composed = composeDescription({
            base: baseDesc,
            lineage: getLineageText(key, columnMetadata.lineage),
            typicalUse: columnMetadata.typicalUse[key] || null,
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
 * Builds the full table description by combining table-specific sections
 * with shared sections (package attribution, config JSON dump).
 *
 * @param {Object} config - The merged configuration object.
 * @param {string[]} tableSections - Table-specific description sections (provided by the table module).
 * @returns {string} The composed table description.
 */
const buildTableDescription = (config, tableSections) => {
    const sections = [...tableSections];

    // Package Attribution
    sections.push(`${constants.TABLE_DESCRIPTION_SUFFIX}\n${constants.TABLE_DESCRIPTION_DOCUMENTATION_LINK}`);

    // Config JSON dump
    const configForDump = Object.fromEntries(
        Object.entries(config).filter(([key]) => !key.startsWith('default'))
    );
    // Strip description and columns from dataformTableConfig to avoid circular reference and bloat
    if (configForDump.dataformTableConfig) {
        const { description, columns, ...rest } = configForDump.dataformTableConfig;
        configForDump.dataformTableConfig = rest;
    }
    const configJson = JSON.stringify(configForDump, null, 2);
    sections.push(`The last full table refresh was done using this configuration:\n${configJson}`);

    return sections.join('\n\n');
};

module.exports = {
    getColumnDescriptions,
    buildTableDescription,
    composeDescription,
    getLineageText,
    buildConfigNotes,
    isExcluded,
};
