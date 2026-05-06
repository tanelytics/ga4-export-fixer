# Sprint Plan: Item List Attribution

## Summary
Implement optional item list attribution for the enhanced GA4 events table — a new `item_list_data` CTE that attributes `item_list_name`, `item_list_id`, and `item_list_index` from list/promotion events to downstream ecommerce events using a configurable lookback window.

**Duration:** 3 milestones (~4-6 hours)
**Dependencies:** None
**Risk Level:** Medium — `array_agg(SELECT AS STRUCT ... REPLACE)` syntax needs BigQuery validation before full implementation

## Proposed Milestones

### Milestone 1: Foundation — Config, Validation, Ecommerce Events Constant

**Goal:** Add the config default, validation rules, shared ecommerce events list, and validation tests. After this milestone, the configuration surface is complete and tested.

**Estimated:** ~80 LOC implementation + ~60 LOC tests = ~140 LOC

**Tasks:**
- [ ] Add `itemListAttribution: undefined` default to `tables/ga4EventsEnhanced/config.js`
- [ ] Add `ga4EcommerceEvents` array constant to `helpers/ga4Transforms.js` (all 14 official GA4 ecommerce events including `refund`)
- [ ] Export the constant from `helpers/ga4Transforms.js`
- [ ] Add validation rules for `itemListAttribution` in `tables/ga4EventsEnhanced/validation.js`:
  - Optional; must be `undefined` or an object
  - `lookbackType` required when object is provided; must be `'SESSION'` or `'TIME'`
  - `lookbackTimeMs` required when `lookbackType` is `'TIME'`; must be a positive integer
  - `lookbackTimeMs` rejected when `lookbackType` is `'SESSION'`
- [ ] Add validation tests in `tests/inputValidation.test.js` (7 test cases from design doc)

**Acceptance Criteria:**
- [ ] `itemListAttribution: undefined` passes validation (disabled by default)
- [ ] Invalid configurations are rejected with descriptive error messages
- [ ] `ga4EcommerceEvents` constant is exported and contains all 14 events
- [ ] All existing tests still pass (`npm test`)

**Files:**
| File | Change |
|------|--------|
| `tables/ga4EventsEnhanced/config.js` | +1 line |
| `tables/ga4EventsEnhanced/validation.js` | +25 lines |
| `helpers/ga4Transforms.js` | +15 lines |
| `tests/inputValidation.test.js` | +60 lines |

---

### Milestone 2: Core — Helper Function and SQL Generation

**Goal:** Implement the attribution helper and wire it into the step pipeline. After this milestone, enabling `itemListAttribution` produces the correct SQL with the `item_list_data` CTE.

**Estimated:** ~120 LOC implementation

**Tasks:**
- [ ] Add `itemListAttributionExpr()` helper to `helpers/ga4Transforms.js` — generates the `LAST_VALUE` window function SQL over a struct, parameterized by lookback type, lookback time, and timestamp column
- [ ] In `tables/ga4EventsEnhanced/index.js` (`_generateEnhancedEventsSQL`):
  - When `itemListAttribution` is enabled, add `_event_row_id` (`ROW_NUMBER() OVER()`) to `eventDataStep.columns`
  - Build the `item_list_data` step: subquery (unnest + window function filtered to ecommerce events excluding refund) + outer query (`array_agg` with `SELECT AS STRUCT ... REPLACE`) grouped by `_event_row_id`
  - Insert `itemListDataStep` into steps array between `eventDataStep` and `sessionDataStep`
  - In `finalStep`, add LEFT JOIN to `item_list_data` on `_event_row_id`, replace `items` with `item_list_data.items`, exclude `_event_row_id` from output
- [ ] Implement `bufferDays` auto-adjustment: when `lookbackType` is `'TIME'`, compute `effectiveBufferDays` and pass it to `ga4ExportDateFilters`
- [ ] Verify disabled path: when `itemListAttribution` is `undefined`, generated SQL is identical to current output

**Acceptance Criteria:**
- [ ] Disabled: no `item_list_data` CTE, no `_event_row_id`, identical SQL output
- [ ] Session mode: CTE present with `partition by session_id, item.item_id`, `rows between unbounded preceding and current row`
- [ ] Time mode: CTE present with `partition by user_pseudo_id, item.item_id`, `range between <microseconds> preceding and current row`
- [ ] `bufferDays` auto-adjusts when time-based lookback exceeds it
- [ ] Uses `event_custom_timestamp` when `customTimestampParam` is configured
- [ ] Ecommerce event filter excludes `refund`

**Risks:**
- `array_agg(SELECT AS STRUCT item.* REPLACE(...))` syntax may not work in BigQuery — validate with a dry-run early. If it fails, fall back to a two-CTE approach (separate `item_attribution` and `item_list_data` CTEs).

**Files:**
| File | Change |
|------|--------|
| `helpers/ga4Transforms.js` | +40 lines |
| `tables/ga4EventsEnhanced/index.js` | +60 lines |

---

### Milestone 3: Verification — SQL Validation Tests and Documentation

**Goal:** Add BigQuery dry-run validation tests for all attribution configurations and document the feature in README.

**Estimated:** ~60 LOC tests + ~20 LOC docs = ~80 LOC

**Tasks:**
- [ ] Add SQL generation test configurations in `tests/ga4EventsEnhanced.test.js`:
  - Session-based attribution
  - Time-based attribution
  - Time-based with `bufferDays` lower than lookback (auto-adjustment)
  - Attribution with `customTimestampParam`
- [ ] Run full test suite (`npm test`) and fix any issues
- [ ] Add `itemListAttribution` section to `README.md`:
  - Config table row
  - Usage examples (disabled, session, time)
  - Note about performance and opt-in nature
- [ ] Update design doc status

**Acceptance Criteria:**
- [ ] All new SQL configurations pass BigQuery dry-run validation
- [ ] All existing tests still pass
- [ ] README documents the new config option with examples
- [ ] `npm test` passes clean

**Files:**
| File | Change |
|------|--------|
| `tests/ga4EventsEnhanced.test.js` | +40 lines |
| `README.md` | +20 lines |

## Success Metrics
- All existing tests passing (`npm test`)
- 4 new SQL validation test configurations passing
- 7 new validation test cases passing
- README updated with new config option

## Open Questions
- Validate `array_agg(SELECT AS STRUCT item.* REPLACE(...))` syntax in BigQuery before Milestone 2 — this is the highest-risk item and determines whether we use one CTE or two
