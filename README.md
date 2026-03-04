# ga4-export-fixer

Helpers and table definitions for processing GA4 (Google Analytics 4) BigQuery export data in **Dataform**.

## Installation

### Bash

```bash
npm install ga4-export-fixer
```

### In Google Cloud Dataform

Include the package in the package.json file in your Dataform repository.

**`package.json`**
```json
{
  "name": "my_dataform_repo",
  "dependencies": {
    "@dataform/core": "3.0.39",
    "ga4-export-fixer": "0.1.1"
  }
}
```
In Google Cloud Dataform, click "Install Packages" to install it in your development workspace.

If your Dataform repository does not have a package.json file, see this guide: https://docs.cloud.google.com/dataform/docs/manage-repository#move-to-package-json

## Usage

### Create GA4 Events Enhanced Table

Creates an **enhanced** version of the GA4 BigQuery export (daily & intraday).

The main features include:

- **Best available data at any time** – Combines daily (processed) and intraday exports so the most complete, accurate version of the data is always available
- **Robust incremental updates** – Run on any schedule (daily, hourly, or custom)
- **Flexible schema, better optimized for analysis** – Keeps the flexible structure of the original export while promoting key fields (e.g. `page_location`, `session_id`) to columns for faster queries; **partitioning and clustering** enabled
- **Event parameter handling** – Promote event params to columns; include or exclude by name
- **Session parameters** – Promote selected event parameters as session-level parameters

#### JS Deployment (Recommended)

Create a new **ga4_events_enhanced** table using a **.js** file in your repository's **definitions** folder.

**`definitions/ga4/ga4_events_enhanced.js`**
```javascript
const { ga4EventsEnhanced } = require('ga4-export-fixer');

const config = {
  sourceTable: constants.GA4_TABLES.MY_GA4_EXPORT
};

ga4EventsEnhanced.createTable(publish, config);
```

#### SQLX Deployment

Alternatively, you can create the **ga4_events_enhanced** table using a .SQLX file.

**`definitions/ga4/ga4_events_enhanced.sqlx`**
```javascript
config {
  type: "incremental",
  description: "GA4 Events Enhanced table",
  schema: "ga4",
  bigquery: {
    partitionBy: "event_date",
    clusterBy: ['event_name', 'session_id', 'page_location', 'data_is_final'],
  },
  tags: ['ga4_export_fixer']
}

js {
  const { ga4EventsEnhanced } = require('ga4-export-fixer');

  const config = {
    sourceTable: ref(constants.GA4_TABLES.MY_GA4_EXPORT),
    self: self(),
    incremental: incremental()
  };
}

${ga4EventsEnhanced.generateSql(config)}

pre_operations {
  ${ga4EventsEnhanced.setPreOperations(config)}
}
```



#### Configuration Object

All fields are optional except `sourceTable`. Default values are applied automatically, so you only need to specify the fields you want to override.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sourceTable` | Dataform ref() / string | **required** | Source GA4 export table. Use `ref()` in Dataform or a string in format `` `project.dataset.table` `` |
| `self` | Dataform self() | **required for .SQLX deployment** | Reference to the table itself. Use `self()` in Dataform |
| `incremental` | Dataform incremental() | **required for .SQLX deployment** | Switch between incremental and full refresh logic. Use `incremental()` in Dataform |
| `schemaLock` | string (YYYYMMDD) | `undefined` | Lock the table schema to a specific date. Must be a valid date >= `"20241009"` |
| `timezone` | string | `'Etc/UTC'` | IANA timezone for event datetime (e.g. `'Europe/Helsinki'`) |
| `customTimestampParam` | string | `undefined` | Name of a custom event parameter containing a JS timestamp in milliseconds (e.g. collected via `Date.now()`) |
| `bufferDays` | integer | `1` | Extra days to include for sessions that span midnight |
| `test` | boolean | `false` | Enable test mode (uses `testConfig` date range instead of pre-operations) |
| `excludedEventParams` | string[] | `[]` | Event parameter names to exclude from the `event_params` array |
| `excludedEvents` | string[] | `[]` | Event names to exclude from the table |
| `excludedColumns` | string[] | `[]` | Default GA4 export columns to exclude from the final table, for example `'app_info'` or `'publisher'` |
| `sessionParams` | string[] | `[]` | Event parameter names to aggregate as session-level parameters |

**`includedExportTypes`** — which GA4 export types to include:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `includedExportTypes.daily` | boolean | `true` | Include daily (processed) export |
| `includedExportTypes.intraday` | boolean | `true` | Include intraday export |

**`dataIsFinal`** — how to determine whether data is final (not expected to change):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dataIsFinal.detectionMethod` | string | `'EXPORT_TYPE'` | `'EXPORT_TYPE'` (uses table suffix) or `'DAY_THRESHOLD'` (uses days since event) |
| `dataIsFinal.dayThreshold` | integer | `4` | Days after which data is considered final. Required when `detectionMethod` is `'DAY_THRESHOLD'` |

**`testConfig`** — date range used when `test` is `true`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `testConfig.dateRangeStart` | string (SQL date) | `'current_date()-1'` | Start date for test queries |
| `testConfig.dateRangeEnd` | string (SQL date) | `'current_date()'` | End date for test queries |

**`preOperations`** — date range and incremental refresh configuration:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `preOperations.dateRangeStartFullRefresh` | string (SQL date) | `'date(2000, 1, 1)'` | Start date for full refresh |
| `preOperations.dateRangeEnd` | string (SQL date) | `'current_date()'` | End date for queries |
| `preOperations.numberOfPreviousDaysToScan` | integer | `10` | Number of previous days to scan from the result table when determining the incremental refresh start checkpoint. A higher value is required if the table updates have fallen behind for some reason |
| `preOperations.incrementalStartOverride` | string (SQL date) | `undefined` | Override the incremental start date to re-process a specific range |
| `preOperations.incrementalEndOverride` | string (SQL date) | `undefined` | Override the incremental end date to re-process a specific range |

**`eventParamsToColumns`** — each item in the array is an object:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Event parameter name |
| `type` | string | No | Data type: `'string'`, `'int'`, `'int64'`, `'double'`, `'float'`, or `'float64'`. If omitted, returns the value converted to a string |
| `columnName` | string | No | Column name in the output. Defaults to the parameter `name` |

Date fields (`dateRangeStart`, `dateRangeEnd`, etc.) accept string dates in `YYYYMMDD` or `YYYY-MM-DD` format, or BigQuery SQL expressions (e.g. `'current_date()'`, `'date(2026, 1, 1)'`).

### Helpers

The helpers contain templates for common SQL expression needed when working with GA4 data.

```javascript
const { helpers } = require('ga4-export-fixer');

// Unnest event parameters, date filters, URL extraction, session aggregation, etc.
helpers.unnestEventParam('page_location', 'string');
helpers.ga4ExportDateFilter('daily', 'current_date()-7', 'current_date()');
helpers.extractPageDetails();
```

## License

MIT
