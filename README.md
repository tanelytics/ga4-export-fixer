# ga4-export-fixer

Helpers and table definitions for processing GA4 (Google Analytics 4) BigQuery export data in **Dataform**.

## Installation
Include the package in the package.json file in your Dataform repository.

```json
{
  "name": "my_dataform_repo",
  "dependencies": {
    "@dataform/core": "3.0.39",
    "ga4-export-fixer": "0.1.0"
  }
}
```
In Google Cloud Dataform, click "Install Packages".

## Usage

### GA4 Events Enhanced Table (JS deployment)

Creates an **enhanced** version of the GA4 BigQuery export (daily & intraday).

Use `ga4EventsEnhanced` to generate the incremental **ga_events_enhanced** table:

```javascript
const { ga4EventsEnhanced } = require('ga4-export-fixer');

const config = {
  sourceTable: constants.GA4_TABLES.MY_GA4_EXPORT
};

// Create a Dataform table (inside a JS file)
ga4EventsEnhanced.createTable(publish, config);
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
