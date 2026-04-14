const helpers = require('../../helpers/index.js');
const utils = require('../../utils.js');
const preOperations = require('../../preOperations.js');
const constants = require('../../constants.js');
const { ga4EventsEnhancedConfig } = require('./config.js');
const { validateEnhancedEventsConfig } = require('./validation.js');
const documentation = require('../../documentation.js');
const { createTable } = require('../../createTable.js');
const { getTableDescriptionSections } = require('./tableDescription.js');

// Column metadata for the GA4 Events Enhanced table
const columnMetadata = {
    descriptions: require('./columns/columnDescriptions.json'),
    lineage: require('./columns/columnLineage.json'),
    typicalUse: require('./columns/columnTypicalUse.json'),
};

// default configuration for the GA4 Events Enhanced table
const defaultConfig = {
    ...ga4EventsEnhancedConfig,
};

// List the columns in the order they should be in the final table
/**
 * Returns an object representing the desired final column order for an enhanced GA4 events table,
 * mapping each column name to its corresponding reference in either the eventDataStep or sessionDataStep.
 *
 * The function defines the preferred order of columns in the final output, grouped into logical sections:
 * - Date and time columns
 * - Event name
 * - Identifiers (such as user and session IDs)
 * - Page-related columns
 * - GA4 parameter columns
 * - Ecommerce columns
 * - Traffic source columns
 *
 * Each column is mapped to the correct data step (event or session), as determined by the presence
 * of that column within the supplied `eventDataStep.columns` or `sessionDataStep.columns` objects.
 * The value for each entry follows the format "{step.name}.{column}" to allow precise SQL referencing.
 *
 * @param {Object} eventDataStep - An object containing a `name` property (string) and a `columns` property (object)
 *   listing available columns from the event-level data step.
 * @param {Object} sessionDataStep - An object containing a `name` property (string) and a `columns` property (object)
 *   listing available columns from the session-level data step.
 * @returns {Object} An ordered mapping of column names to their qualified step/column references.
 *
 * @example
 * const order = getFinalColumnOrder(eventStep, sessionStep);
 * // order = { event_date: "event_data.event_date", ..., user_traffic_source: "session_data.user_traffic_source" }
 */
const getFinalColumnOrder = (eventDataStep, sessionDataStep) => {
    const dateAndTimeColumns = [
        'event_date',
        'event_datetime',
        'event_timestamp',
        'event_custom_timestamp',
    ];

    const eventColumns = [
        'event_name',
    ];

    const identifierColumns = [
        'session_id',
        'user_pseudo_id',
        'user_id',
        'merged_user_id',
    ];

    const pageColumns = [
        'page_location',
        'page',
        'landing_page',
    ];

    const parameterColumns = [
        'event_params',
        'session_params',
        'user_properties',
    ];

    const ecommerceColumns = [
        'ecommerce',
        'items',
        'user_ltv',
    ];

    const trafficSourceColumns = [
        'collected_traffic_source',
        'session_first_traffic_source',
        'session_traffic_source_last_click',
        'user_traffic_source',
    ];

    const finalColumnOrder = [
        ...dateAndTimeColumns,
        ...eventColumns,
        ...identifierColumns,
        ...pageColumns,
        ...parameterColumns,
        ...ecommerceColumns,
        ...trafficSourceColumns,
    ];

    // Construct the columns object: key is column name, value is {step.name}.{column}
    const columnOrder = {};
    for (const col of finalColumnOrder) {
        if (sessionDataStep?.columns?.hasOwnProperty(col) && sessionDataStep.columns[col] !== undefined) {
            columnOrder[col] = `${sessionDataStep.name}.${col}`;
        } else if (eventDataStep?.columns?.hasOwnProperty(col) && eventDataStep.columns[col] !== undefined) {
            columnOrder[col] = `${eventDataStep.name}.${col}`;
        }
    }
    return columnOrder;
};

/**
 * Generates a SQL query for an enhanced version of GA4 event export data.
 *
 * This function constructs a modular, multi-step SQL query—represented as a series of steps—that transforms raw GA4 event export data into a more analytics-ready "enhanced" format.
 * It includes configurable event filtering, exclusion of columns and events, promotion of event parameters to top-level columns, date/timestamp handling, session-level aggregation,
 * enrichment with user, page, traffic source, and ecommerce details, as well as optional session parameters and data freshness flags.
 * 
 * The resulting query can be run directly or used as a basis for table creation (e.g. via Dataform).
 *
 * @param {Object} config                                   - Configuration object for the export enhancement process.
 *   @param {string}   config.sourceTable                   - The BigQuery table name or table reference string to use as the source of event data. (Required)
 *   @param {string[]} [config.defaultExcludedEvents]        - Events to always exclude, in addition to those in `excludedEvents`.
 *   @param {string[]} [config.excludedEvents]               - Additional event_name values to exclude from the results.
 *   @param {string[]} [config.defaultExcludedEventParams]   - Event parameters to exclude from the array of event_params.
 *   @param {string[]} [config.excludedEventParams]          - Additional event parameters to exclude from event_params.
 *   @param {string[]} [config.excludedColumns]              - Source table columns to omit from the select list completely.
 *   @param {Object[]} [config.eventParamsToColumns]         - List of event parameter definitions to promote to top-level columns. Each should include:
 *       @param {string}  name                               - Event parameter name to promote.
 *       @param {string}  [columnName]                       - Optional resulting column name (defaults to parameter name).
 *       @param {string}  type                               - Data type for unnesting.
 *   @param {string[]} [config.sessionParams]                - List of parameter names to aggregate at the session level (optional).
 *   @param {string}   [config.customTimestampParam]         - If provided, the name of a custom timestamp event parameter to be used.
 *   @param {string}   [config.timezone]                     - Output timezone for event datetime columns.
 *   @param {Object}   [config.dataIsFinal]                  - Determines the logic for `data_is_final` flag, with fields:
 *       @param {string} detectionMethod                      - Method for detecting finality of records.
 *       @param {number} dayThreshold                         - Number of days for finality consideration.
 *   @param {...*}     [config.*]                            - Any additional keys are passed through to helpers and utilities.
 * 
 * @returns {string} SQL query string representing the transformation pipeline for enhanced GA4 event data.
 * 
 * @throws {Error} If the required `sourceTable` configuration is missing or invalid.
 * 
 * @example
 * // Returns a SQL string for an enhanced events table
 * const sql = generateEnhancedEventsSQL({
 *   sourceTable: '`myproject.my_dataset.analytics_123456789.events_*`',
 *   excludedEvents: ['user_engagement'],
 *   excludedColumns: ['event_bundle_sequence_id'],
 *   eventParamsToColumns: [{ name: 'foo', type: 'string' }]
 * });
 */
const _generateEnhancedEventsSQL = (mergedConfig) => {
    // the most accurate available timestamp column
    const timestampColumn = mergedConfig.customTimestampParam ? 'event_custom_timestamp' : 'event_timestamp';

    // item list attribution config
    const itemListAttribution = mergedConfig.itemListAttribution;

    // auto-adjust bufferDays for time-based item list attribution lookback
    const effectiveBufferDays = (itemListAttribution && itemListAttribution.lookbackType === 'TIME')
        ? Math.max(mergedConfig.bufferDays, Math.ceil(itemListAttribution.lookbackTimeMs / (24 * 60 * 60 * 1000)))
        : mergedConfig.bufferDays;
    const dateFilterConfig = effectiveBufferDays !== mergedConfig.bufferDays
        ? { ...mergedConfig, bufferDays: effectiveBufferDays }
        : mergedConfig;

    // exlude these events from the table
    const excludedEvents = mergedConfig.excludedEvents;
    const excludedEventsSQL = excludedEvents.length > 0 ? `and event_name not in (${excludedEvents.map(event => `'${event}'`).join(',')})` : '';

    // promote these event parameters to columns
    const promotedEventParameters = () => {
        const promotedParameters = {};
        mergedConfig.eventParamsToColumns.forEach(p => {
            const columnName = p.columnName || p.name;
            promotedParameters[columnName] = helpers.unnestEventParam(p.name, p.type);
        });
        return promotedParameters;
    };

    const getExcludedColumns = () => {
        const allExcludedColumns = mergedConfig.excludedColumns;
        const excludedColumns = {};
        allExcludedColumns.forEach(c => {
            excludedColumns[c] = undefined;
        });
        return excludedColumns;
    };

    // initial step: extract data from the export tables
    const eventDataStep = {
        name: 'event_data',
        columns: {
            // exclude default export columns that are not needed
            // do this first so that the columns defined later are not excluded
            ...getExcludedColumns(),
            // date and time
            event_date: helpers.eventDate,
            event_datetime: `extract(datetime from timestamp_micros(${helpers.getEventTimestampMicros(mergedConfig.customTimestampParam)}) at time zone '${mergedConfig.timezone}')`,
            event_timestamp: 'event_timestamp',
            event_custom_timestamp: mergedConfig.customTimestampParam ? helpers.getEventTimestampMicros(mergedConfig.customTimestampParam) : undefined,
            // event name
            event_name: 'event_name',
            // identifiers
            session_id: helpers.sessionId,
            user_pseudo_id: 'user_pseudo_id',
            user_id: 'user_id',
            // page
            page_location: helpers.unnestEventParam('page_location', 'string'),
            page: helpers.extractPageDetails(),
            // event parameters and user properties
            ...promotedEventParameters(),
            event_params: helpers.filterEventParams(mergedConfig.excludedEventParams, 'exclude'),
            user_properties: 'user_properties',
            // traffic source
            collected_traffic_source: 'collected_traffic_source',
            session_traffic_source_last_click: 'session_traffic_source_last_click',
            user_traffic_source: 'traffic_source',
            // ecommerce
            ecommerce: helpers.fixEcommerceStruct('ecommerce'),
            items: 'items',
            // unique row id for item list attribution join.
            // row_number() over() breaks hash collisions for batched events with identical data.
            // Non-determinism is safe: colliding rows have identical items (to_json_string(items) is in the hash),
            // so swapping row numbers between them produces the same final result.
            _event_row_id: itemListAttribution ? `farm_fingerprint(concat(user_pseudo_id, cast(event_timestamp as string), event_name, to_json_string(items), cast(row_number() over() as string)))` : undefined,
            // flag if the data is "final" and is not expected to change anymore
            data_is_final: helpers.isFinalData(mergedConfig.dataIsFinal.detectionMethod, mergedConfig.dataIsFinal.dayThreshold),
            export_type: helpers.getGa4ExportType('_table_suffix'),
            // prep columns for later steps
            entrances: helpers.unnestEventParam('entrances', 'int'),
            session_params_prep: mergedConfig.sessionParams.length > 0 ? helpers.filterEventParams(mergedConfig.sessionParams, 'include') : undefined,
            // include all other columns from the export data
            get '[sql]other_columns'() {
                const definedColumns = Object.keys(this);
                return `* except (${definedColumns.filter(column => helpers.isGa4ExportColumn(column)).join(', ')})`;
            },
        },
        from: mergedConfig.sourceTable,
        where: `${helpers.ga4ExportDateFilters(dateFilterConfig)}
${excludedEventsSQL}`,
    };

    // Do session-level data aggregation
    const sessionDataStep = {
        name: 'session_data',
        columns: {
            session_id: 'session_id',
            user_id: helpers.aggregateValue('user_id', 'last', timestampColumn),
            merged_user_id: `ifnull(${helpers.aggregateValue('user_id', 'last', timestampColumn)}, any_value(user_pseudo_id))`,
            session_params: helpers.aggregateSessionParams(mergedConfig.sessionParams, 'session_params_prep', timestampColumn),
            session_traffic_source_last_click: helpers.aggregateValue('session_traffic_source_last_click', 'first', timestampColumn),
            session_first_traffic_source: `array_agg(collected_traffic_source order by ${timestampColumn} limit 1)[safe_offset(0)]`, // don't ignore nulls
            landing_page: helpers.aggregateValue(`if(entrances > 0, page, null)`, 'first', timestampColumn),
        },
        from: 'event_data',
        where: `session_id is not null`,
        groupBy: ['session_id']
    };

    // item list attribution CTE: unnest items, attribute via window function, re-aggregate
    const itemListDataStep = itemListAttribution ? (() => {
        const attrExpr = helpers.itemListAttributionExpr(
            itemListAttribution.lookbackType,
            timestampColumn,
            itemListAttribution.lookbackTimeMs
        );
        const passthroughEvents = `event_name in ('view_item_list', 'select_item', 'view_promotion', 'select_promotion')`;
        const ecommerceFilter = helpers.ga4EcommerceEvents.filter(e => e !== 'refund').map(e => `'${e}'`).join(', ');

        return {
            name: 'item_list_data',
            columns: {
                '_event_row_id': '_event_row_id',
                'items': `array_agg(
      (select as struct item.* replace(
        coalesce(if(${passthroughEvents}, item.item_list_name, _item_list_attr.item_list_name), '(not set)') as item_list_name,
        coalesce(if(${passthroughEvents}, item.item_list_id, _item_list_attr.item_list_id), '(not set)') as item_list_id,
        coalesce(if(${passthroughEvents}, item.item_list_index, _item_list_attr.item_list_index)) as item_list_index
      ))
    )`,
            },
            from: `(select _event_row_id, event_name, item, ${attrExpr} as _item_list_attr from event_data, unnest(items) as item where event_name in (${ecommerceFilter}))`,
            groupBy: ['_event_row_id'],
        };
    })() : null;

    const finalColumnOrder = getFinalColumnOrder(eventDataStep, sessionDataStep);

    // When item list attribution is enabled, override the items column and exclude _event_row_id
    // COALESCE handles events without items (not in ecommerce filter) where the LEFT JOIN returns NULL
    const itemListOverrides = itemListDataStep ? {
        items: 'coalesce(item_list_data.items, event_data.items)',
    } : {};
    const itemListExcludedColumns = itemListDataStep ? ['_event_row_id'] : [];

    // Join event_data and session_data, include additional logic
    const finalStep = {
        name: 'final',
        columns: {
            // get the most important columns in the correct order
            ...finalColumnOrder,
            ...itemListOverrides,
            // get the rest of the event_data columns
            '[sql]event_data': utils.selectOtherColumns(
                eventDataStep,
                Object.keys(finalColumnOrder),
                [
                    'entrances',
                    mergedConfig.sessionParams.length > 0 ? 'session_params_prep' : undefined,
                    'data_is_final',
                    'export_type',
                    ...itemListExcludedColumns,
                ]
            ),
            // get the rest of the session_data columns
            '[sql]session_data': utils.selectOtherColumns(
                sessionDataStep,
                Object.keys(finalColumnOrder),
                []
            ),
            // include additional columns
            row_inserted_timestamp: 'current_timestamp()',
            data_is_final: 'data_is_final',
            export_type: 'export_type',
        },
        from: 'event_data',
        leftJoin: [
            ...(itemListDataStep ? [{
                table: 'item_list_data',
                condition: 'using(_event_row_id)'
            }] : []),
            {
                table: 'session_data',
                condition: 'using(session_id)'
            }
        ],
        where: helpers.incrementalDateFilter(mergedConfig)
    };

    const steps = [
        eventDataStep,
        ...(itemListDataStep ? [itemListDataStep] : []),
        sessionDataStep,
        finalStep,
    ];

    return utils.queryBuilder(steps);
};

// Exported wrapper: merge config, validate, then delegate to the internal function
const generateEnhancedEventsSQL = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    validateEnhancedEventsConfig(mergedConfig);
    return _generateEnhancedEventsSQL(mergedConfig);
};

/**
 * Creates an enhanced GA4 events table using Dataform's publish() API.
 *
 * This function merges the provided configuration with a default configuration,
 * sets up the Dataform table configuration (including partitioning, clustering,
 * schema, and description), and publishes an incremental table using Dataform.
 * Additional pre-operations and custom SQL generation are attached via the
 * Dataform API hooks.
 *
 * The default table name and schema are determined based on the sourceTable
 * configuration. These can be overridden by providing custom values in the
 * config or its nested dataformTableConfig.
 *
 * @param {Function} dataformPublish - The Dataform publish() function to create the table.
 * @param {Object} config - User-provided configuration for the enhanced table and merge settings.
 *   @param {Object} [config.dataformTableConfig] - Optional table configuration overrides for Dataform.
 *   @param {Object|string} config.sourceTable - Source table, either a Dataform table reference object or a string ('`project.dataset.table`').
 *   @param {Object} [config.*] - Any additional supported configuration values.
 *
 * @returns {Object} The Dataform publish() object for the enhanced events table, supporting chaining (e.g. .preOps, .query).
 */
// Table module definition — conforms to the shared createTable interface
const tableModule = {
    defaultConfig,
    defaultTableName: constants.DEFAULT_EVENTS_TABLE_NAME,
    validate: validateEnhancedEventsConfig,
    generateSql: _generateEnhancedEventsSQL,
    getColumnDescriptions: (config) => documentation.getColumnDescriptions(config, columnMetadata),
    getTableDescription: (config) => documentation.buildTableDescription(config, getTableDescriptionSections(config)),
};

const createEnhancedEventsTable = (dataformPublish, config) => {
    return createTable(dataformPublish, config, tableModule);
};

// Exported wrapper: merge config, validate, then delegate to preOperations module
const setPreOperations = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    validateEnhancedEventsConfig(mergedConfig);
    return preOperations.setPreOperations(mergedConfig);
};

const getColumnDescriptions = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    return documentation.getColumnDescriptions(mergedConfig, columnMetadata);
};

const getTableDescription = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    return documentation.buildTableDescription(mergedConfig, getTableDescriptionSections(mergedConfig));
};

module.exports = {
    createTable: createEnhancedEventsTable,
    generateSql: generateEnhancedEventsSQL,
    setPreOperations: setPreOperations,
    getColumnDescriptions: getColumnDescriptions,
    getTableDescription: getTableDescription
}