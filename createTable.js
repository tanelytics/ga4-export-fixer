const utils = require('./utils.js');
const preOperations = require('./preOperations.js');

/**
 * Shared createTable lifecycle for all table modules.
 *
 * Merges configuration, validates it, builds the Dataform table config
 * (name, schema, columns, description), and publishes the table with
 * pre-operations and the module's SQL query.
 *
 * @param {Function} dataformPublish - Dataform publish() function.
 * @param {Object} userConfig - User-provided configuration.
 * @param {Object} tableModule - Table module definition conforming to the table module interface:
 *   @param {Object}   tableModule.defaultConfig - Default config extending baseConfig.
 *   @param {string}   tableModule.defaultTableName - Default table name (e.g. 'ga4_events_enhanced').
 *   @param {Function} tableModule.validate - (mergedConfig, options?) => void.
 *   @param {Function} tableModule.generateSql - (mergedConfig) => string.
 *   @param {Function} tableModule.getColumnDescriptions - (mergedConfig) => Dataform columns object.
 *   @param {Function} tableModule.getTableDescription - (mergedConfig) => string.
 * @returns {Object} The Dataform publish() object for the table.
 */
const createTable = (dataformPublish, userConfig, tableModule) => {
    const mergedConfig = utils.mergeSQLConfigurations(tableModule.defaultConfig, userConfig);
    tableModule.validate(mergedConfig, { skipDataformContextFields: true });

    const dataset = utils.getDatasetName(mergedConfig.sourceTable);

    // Build dataformTableConfig: static defaults → dynamic fields → user overrides.
    // Deep-clone defaults to prevent Dataform's publish() from mutating nested objects (e.g. bigquery)
    // across multiple createTable calls in the same process.
    const dataformTableConfig = utils.mergeDataformTableConfigurations(
        {
            ...JSON.parse(JSON.stringify(tableModule.defaultConfig.dataformTableConfig || {})),
            name: `${tableModule.defaultTableName}_${dataset.replace('analytics_', '')}`,
            schema: dataset,
            columns: tableModule.getColumnDescriptions(mergedConfig),
        },
        userConfig.dataformTableConfig
    );

    // Pass dataformTableConfig to getTableDescription via a new object to avoid mutating mergedConfig
    // (Dataform's sandboxed runtime may freeze objects returned by mergeSQLConfigurations)
    const tableDescription = tableModule.getTableDescription({ ...mergedConfig, dataformTableConfig });

    // Set description (user override from the merge wins if provided)
    if (!dataformTableConfig.description) {
        dataformTableConfig.description = tableDescription;
    }

    // Create the table using Dataform publish()
    return dataformPublish(dataformTableConfig.name, dataformTableConfig).preOps(ctx => {
        return preOperations.setPreOperations(utils.setDataformContext(ctx, mergedConfig));
    }).query(ctx => {
        return tableModule.generateSql(utils.setDataformContext(ctx, mergedConfig));
    });
};

module.exports = { createTable };
