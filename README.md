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
    "ga4-export-fixer": "0.1.0"
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
