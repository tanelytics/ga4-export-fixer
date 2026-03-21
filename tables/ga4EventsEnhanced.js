const helpers = require('../helpers.js');
const utils = require('../utils.js');
const inputValidation = require('../inputValidation.js');
const constants = require('../constants.js');
const preOperations = require('../preOperations.js');
const { ga4EventsEnhancedConfig } = require('../defaultConfig.js'); // config defaults

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
const generateEnhancedEventsSQL = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);

    // validate the config and throw an error if it's invalid
    inputValidation.validateEnhancedEventsConfig(mergedConfig);

    if (!mergedConfig.sourceTable || typeof mergedConfig.sourceTable !== 'string' || mergedConfig.sourceTable.trim() === '') {
        throw new Error("generateEnhancedEventsSQL: 'sourceTable' is a required parameter in config and must be a non-empty string.");
    }

    // the most accurate available timestamp column
    const mainTimestampColumn = mergedConfig.customTimestampParam ? 'event_custom_timestamp' : 'event_timestamp';

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
        where: `${helpers.ga4ExportDateFilters(mergedConfig)}
${excludedEventsSQL}`,
    };

    // Do session-level data aggregation
    const sessionDataStep = {
        name: 'session_data',
        columns: {
            session_id: 'session_id',
            user_id: helpers.aggregateValue('user_id', 'last', mainTimestampColumn),
            merged_user_id: `ifnull(${helpers.aggregateValue('user_id', 'last', mainTimestampColumn)}, any_value(user_pseudo_id))`,
            session_params: helpers.aggregateSessionParams(mergedConfig.sessionParams, 'session_params_prep', mainTimestampColumn),
            session_traffic_source_last_click: helpers.aggregateValue('session_traffic_source_last_click', 'first', mainTimestampColumn),
            session_first_traffic_source: helpers.aggregateValue('collected_traffic_source', 'first', mainTimestampColumn),
            landing_page: helpers.aggregateValue(`if(entrances > 0, page, null)`, 'first', mainTimestampColumn),
        },
        from: 'event_data',
        where: `session_id is not null`,
        groupBy: ['session_id']
    };

    const finalColumnOrder = getFinalColumnOrder(eventDataStep, sessionDataStep);

    // Join event_data and session_data, include additional logic
    const finalStep = {
        name: 'final',
        columns: {
            // get the most important columns in the correct order
            ...finalColumnOrder,
            // get the rest of the event_data columns
            '[sql]event_data': utils.selectOtherColumns(
                eventDataStep, 
                Object.keys(finalColumnOrder),
                [
                    'entrances',
                    mergedConfig.sessionParams.length > 0 ? 'session_params_prep' : undefined,
                    'data_is_final',
                    'export_type',
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
            {
                table: 'session_data',
                condition: 'using(session_id)'
            }
        ],
        where: helpers.incrementalDateFilter(mergedConfig)
    };

    const steps = [
        eventDataStep,
        sessionDataStep,
        finalStep,
    ];

    return utils.queryBuilder(steps);
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
const createEnhancedEventsTable = (dataformPublish, config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);

    const tableDescription = `GA4 Events Enhanced 

- Combines daily (processed) and intraday exports so the best available version of each event is always used.
- Incremental updates: All data with "data_is_final" flag set to false is deleted and replaced with the latest available data on every run (supports any schedule: daily, hourly, or custom).
- Keeps the flexible schema of the original export while promoting key fields (e.g. page_location, session_id) to columns for faster queries.
- Partitioned by event_date and clustered for optimal query performance.
- Event parameter handling: promote params to columns or exclude by name.
- Session-level fields: landing_page, fixed user_id, and configurable session parameters.
- Other improvements and refinements based on configuration

${constants.TABLE_DESCRIPTION_SUFFIX}
${constants.TABLE_DESCRIPTION_DOCUMENTATION_LINK}

The last full table refresh was done using this configuration:
${JSON.stringify(
  Object.fromEntries(
    // don't display the default arrays here, their contents are included in the main arrays via the mergeSQLConfigurations function
    Object.entries(mergedConfig).filter(([key]) => !key.startsWith('default'))
  ),
  null,
  2
)}`;

    // the defaults for the dataform table config
    const defaultDataformTableConfig = {
        name: constants.DEFAULT_EVENTS_TABLE_NAME,
        type: 'incremental',
        schema: 'ga4_export_fixer',
        description: tableDescription,
        bigquery: {
            partitionBy: 'event_date',
            clusterBy: ['event_name', 'session_id', 'page_location', 'data_is_final'],
            labels: {
                'ga4_export_fixer': 'true'
            }
        },
        tags: ['ga4_export_fixer'],
        // todo: include columns object
        columns: {
            event_date: 'Date of the event',
            event_params: {
                description: 'Event parameters array',
                columns: {
                    key: 'Name of the event parameter',
                    value: {
                        description: 'A struct with the value of the event parameter',
                        columns: {
                            string_value: 'String value of the event parameter',
                            int_value: 'Integer value of the event parameter',
                            float_value: 'Float value of the event parameter',
                            double_value: 'Double value of the event parameter',
                        }
                    }
                }
            }
        }
    };

    // set the default values for table name and dataset, if not provided in the config
    const setDefaults = () => {
        const getDatasetName = (sourceTable) => {
            if (utils.isDataformTableReferenceObject(sourceTable)) {
                return sourceTable.dataset || sourceTable.schema;
            }
            if (typeof sourceTable === 'string' && /^`[^\.]+\.[^\.]+\.[^\.]+`$/.test(sourceTable)) {
                return sourceTable.split('.')[1];
            }
            throw new Error(`Unable to extract the dataset name from sourceTable, received: ${JSON.stringify(sourceTable)}`);
        };

        const dataset = getDatasetName(mergedConfig.sourceTable);

        defaultDataformTableConfig.name = `${constants.DEFAULT_EVENTS_TABLE_NAME}_${dataset.replace('analytics_', '')}`;
        defaultDataformTableConfig.schema = dataset;
    };

    setDefaults();

    // merge the dataform table config with the default dataform table config
    const dataformTableConfig = utils.mergeDataformTableConfigurations(defaultDataformTableConfig, mergedConfig.dataformTableConfig);

    // create the table using Dataform publish()
    return dataformPublish(dataformTableConfig.name, dataformTableConfig).preOps(ctx => {
        return preOperations.setPreOperations(utils.setDataformContext(ctx, mergedConfig));
    }).query(ctx => {
        return generateEnhancedEventsSQL(utils.setDataformContext(ctx, mergedConfig));
    });

};

// provide a merged config for the pre operations
// required for the .sqlx deployment
const setPreOperations = (config) => {
    const mergedConfig = utils.mergeSQLConfigurations(defaultConfig, config);
    return preOperations.setPreOperations(mergedConfig);
};

module.exports = {
    generateSql: generateEnhancedEventsSQL,
    createTable: createEnhancedEventsTable,
    setPreOperations: setPreOperations
}