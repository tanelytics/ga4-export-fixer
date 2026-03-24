const columnDescriptions = require('./columns/columnDescriptions.json');

/**
 * Returns a deep copy of the default column descriptions, enriched with
 * configuration-specific context appended to the relevant descriptions.
 *
 * @param {Object} config - The merged configuration object from ga4EventsEnhanced.
 * @returns {Object} Column descriptions object in Dataform ITableConfig columns format.
 */
const getColumnDescriptions = (config) => {
    const descriptions = JSON.parse(JSON.stringify(columnDescriptions));

    if (!config) return descriptions;

    const appendToDescription = (key, suffix) => {
        if (!descriptions[key]) return;
        if (typeof descriptions[key] === 'string') {
            descriptions[key] = `${descriptions[key]}. ${suffix}`;
        } else if (typeof descriptions[key] === 'object' && descriptions[key].description) {
            descriptions[key].description = `${descriptions[key].description}. ${suffix}`;
        }
    };

    // timezone
    if (config.timezone) {
        appendToDescription('event_datetime', `Timezone: ${config.timezone}`);
    }

    // customTimestampParam
    if (config.customTimestampParam) {
        appendToDescription('event_datetime', `Custom timestamp parameter: '${config.customTimestampParam}'`);
        appendToDescription('event_custom_timestamp', `Source parameter: '${config.customTimestampParam}'`);
    } else {
        delete descriptions.event_custom_timestamp;
    }

    // data_is_final
    if (config.dataIsFinal) {
        const method = config.dataIsFinal.detectionMethod;
        if (method === 'DAY_THRESHOLD') {
            appendToDescription('data_is_final', `Detection method: DAY_THRESHOLD (${config.dataIsFinal.dayThreshold} days)`);
        } else {
            appendToDescription('data_is_final', `Detection method: EXPORT_TYPE`);
        }
    }

    // excludedEvents
    if (config.excludedEvents && config.excludedEvents.length > 0) {
        appendToDescription('event_name', `Excluded events: ${config.excludedEvents.join(', ')}`);
    }

    // excludedEventParams
    if (config.excludedEventParams && config.excludedEventParams.length > 0) {
        appendToDescription('event_params', `Excluded parameters: ${config.excludedEventParams.join(', ')}`);
    }

    // sessionParams
    if (config.sessionParams && config.sessionParams.length > 0) {
        appendToDescription('session_params', `Configured parameters: ${config.sessionParams.join(', ')}`);
    }

    // eventParamsToColumns — add descriptions for dynamically promoted columns
    if (config.eventParamsToColumns && config.eventParamsToColumns.length > 0) {
        config.eventParamsToColumns.forEach(p => {
            const columnName = p.columnName || p.name;
            const type = p.type ? ` (${p.type})` : ' (any data type)';
            descriptions[columnName] = `Promoted from event parameter '${p.name}'${type}`;
        });
    }

    // includedExportTypes
    if (config.includedExportTypes) {
        const types = Object.entries(config.includedExportTypes)
            .filter(([, enabled]) => enabled)
            .map(([type]) => type);
        appendToDescription('export_type', `Included export types: ${types.join(', ')}`);
    }

    return descriptions;
};

module.exports = {
    columnDescriptions,
    getColumnDescriptions
};