const constants = require('./constants.js');

const declareVariable = (variable, value) => {
  return `declare ${variable} default (
  ${value}
);`;
};

// Get the last partition date from the result table
const getLastPartitionDate = (config) => {
  const informationSchemaPath = config.self.replace(
    /`?([^`]+)\.([^`]+)\.[^`]+`?$/,
    '`$1.$2.INFORMATION_SCHEMA.PARTITIONS`'
  );
  const tableName = config.self.replace(/`/g, '').split('.').pop();

  return `select 
  max(parse_date('%Y%m%d', partition_id))
from 
  ${informationSchemaPath}
where 
  table_name = '${tableName}' and partition_id != '__NULL__'`;
};

// Define the date range start for incremental and full refresh
const getDateRangeStart = (config) => {
  if (config.incremental) {
    // if an incremental start override is provided, use it
    if (config.preOperations.incrementalStartOverride) {
      return `select ${config.preOperations.incrementalStartOverride}`;
    }

    // otherwise, use the default logic
    return `with days as (
  select
    ${constants.DATE_COLUMN},
    min(data_is_final) as data_is_final
  from
    ${config.self}
  where
    ${constants.DATE_COLUMN} > ${constants.LAST_PARTITION_DATE_VARIABLE}-${config.preOperations.numberOfPreviousDaysToScan}
  group by
    ${constants.DATE_COLUMN}
)
select
  max(${constants.DATE_COLUMN})+1 as ${constants.DATE_RANGE_START_VARIABLE}
from
  days
where
  data_is_final = true`;
  } else {
    const dateRangeStartFullRefresh = config.test ? config.testConfig.dateRangeStart : config.preOperations.dateRangeStartFullRefresh;
    return `select ${dateRangeStartFullRefresh}`;
  }

};

// Define the date range start for incremental refresh with intraday tables
// Uses INFORMATION_SCHEMA.TABLES to avoid scanning actual table data
const getDateRangeStartIntraday = (config) => {
  const getStartDate = () => {
    if (config.incremental) {
      return `greatest(${constants.DATE_RANGE_START_VARIABLE}, current_date()-5)`;
    }
    return 'current_date()-5';
  };

  const startDate = getStartDate();

  if (config.includedExportTypes.intraday) {
    const informationSchemaPath = config.sourceTable.replace(
      /`?([^`]+)\.([^`]+)\.[^`]+`?$/,
      '`$1.$2.INFORMATION_SCHEMA.TABLES`'
    );

    return `with export_statuses as (
    select
      safe_cast(regexp_extract(table_name, r'\\d+') as date format 'YYYYMMDD') as date,
      case
        when table_name like 'events_intraday_%' then 'intraday'
        else 'daily'
      end as export_type
    from
      ${informationSchemaPath}
    where
      regexp_contains(table_name, r'^events_(intraday_)?\\d{8}$')
      and safe_cast(regexp_extract(table_name, r'\\d+') as date format 'YYYYMMDD')
        between ${startDate} and current_date()
  ),
  statuses_by_day as (
    select
      date,
      max(if(export_type = 'daily', true, false)) as daily,
      max(if(export_type = 'intraday', true, false)) as intraday
    from
      export_statuses
    group by 
      date
  )
  select
    min(
      if(
        intraday = true and daily = false,
        date,
        null
      )
    )
  from
    statuses_by_day`;
  }

  return undefined;
};

const getDateRangeEnd = (config) => {
  // if an incremental end override is provided, use it
  if (config.incremental && config.preOperations.incrementalEndOverride) {
    return `select ${config.preOperations.incrementalEndOverride}`;
  }

  // otherwise, use the default logic
  return `select ${config.preOperations.dateRangeEnd}`;
};

const deleteNonFinalRows = (config) => {
  return `delete from ${config.self} where ${constants.DATE_COLUMN} >= ${constants.DATE_RANGE_START_VARIABLE} and ${constants.DATE_COLUMN} <= ${constants.DATE_RANGE_END_VARIABLE};`;
};

const createSchemaLockTable = (config) => {
  const tableName = 'events_schema_lock';
  const tablePath = config.sourceTable.replace(/`?([^`]+)\.([^`]+)\.[^`]+`?$/, `\`$1.$2.${tableName}\``);
  const copySchemaFromTable = config.sourceTable.replace(/`?([^`]+)\.([^`]+)\.[^`]+`?$/, `\`$1.$2.events_${config.schemaLock}\``);
  
  return `create or replace table ${tablePath}
  like ${copySchemaFromTable}
  options(
    description = "Temporary table for locking GA4 export schema to the ${config.schemaLock} version. Auto-expires in 5 minutes.",
    expiration_timestamp = timestamp_add(current_timestamp(), interval 5 minute)
  );`;
};

// Set the pre operations for the query
const setPreOperations = (config) => {
  // if in test mode, avoid setting BigQuery variables to make query dry run estimation accurate
  if (config.test) {
    return '';
  }

  // define the pre operations
  const preOperations = [
    {
      type: 'variable',
      name: constants.LAST_PARTITION_DATE_VARIABLE,
      value: config.incremental ? getLastPartitionDate(config) : undefined,
      comment: 'Get the last partition date from the result table. Used to anchor the incremental date checkpoint scan window to the table\'s actual data.',
    },
    {
      type: 'variable',
      name: constants.DATE_RANGE_START_VARIABLE,
      // variable only needed with incremental refresh
      value: config.incremental ? getDateRangeStart(config) : undefined,
      comment: 'Define the date range start for incremental and full refresh.',
    },
    {
      type: 'variable',
      name: constants.INTRADAY_DATE_RANGE_START_VARIABLE,
      // variable only needed if intraday export tables are included together with daily export tables
      value: config.sourceTableType === 'GA4_EXPORT' && config.includedExportTypes.intraday && config.includedExportTypes.daily ? getDateRangeStartIntraday(config) : undefined,
      comment: 'Define the date range start for intraday export tables. Avoid returning intraday data if it overlaps with daily export data. Only needed if intraday export tables are included together with daily export tables.',
    },
    {
      type: 'variable',
      name: constants.DATE_RANGE_END_VARIABLE,
      // variable only needed with incremental refresh
      value: config.incremental ? getDateRangeEnd(config) : undefined,
      comment: 'Define the date range end.',
    },
    {
      type: 'delete',
      // delete only needed with incremental refresh
      value: config.incremental ? deleteNonFinalRows(config) : undefined,
      comment: 'Delete all rows that are about to be inserted again. (data_is_final = false)',
    },
    {
      type: 'create',
      // create table statement only needed with schema lock
      value: config.sourceTableType === 'GA4_EXPORT' && config.schemaLock ? createSchemaLockTable(config) : undefined,
      comment: 'Lock the schema to a specific version by creating a table copy from the selected day\'s export.'
    },
  ];

  // generate the pre operations SQL
  const preOperationsSQL = preOperations.filter(p => p.value !== undefined).map((p) => {
    if (p.type === 'variable') {
      return `-- ${p.comment}
${declareVariable(p.name, p.value)}`;
    } else if (p.type === 'delete' || p.type === 'create') {
      return `-- ${p.comment}
${p.value}`;
    }
  }).join('\n\n');

  // set the variables in pre operations
  return `
/*
Set the pre-operations for the query, required for managing incremental refreshes.
*/

${preOperationsSQL}

-- End of pre-operations

`;
};

module.exports = {
  setPreOperations
};