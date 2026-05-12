/*
General utility functions
*/

/**
 * Merges multiple arrays into a single array containing only unique values.
 * 
 * - Accepts any number of array arguments, including undefined.
 * - Treats undefined arguments as empty arrays.
 * - Flattens the arrays and removes duplicate values, preserving order of first appearance.
 * 
 * @param {...Array} arrays - Arrays to merge.
 * @returns {Array} The merged array with unique values.
 */
const mergeUniqueArrays = (...arrays) => {
    // Convert undefined values to empty arrays before merging
    const validArrays = arrays.map(arr => arr === undefined ? [] : arr);
    return [...new Set(validArrays.flat())];
};

/**
 * Build SQL query from an array of steps with support for CTEs.
 *
 * Each step is one of two shapes — structured (clause-keyed) or raw (`{name, query}`).
 *
 * STRUCTURED SHAPE:
 *   { name, select, from, joins?, where?, 'group by'?, having?, qualify?, 'order by'?, limit? }
 *
 *   - name: CTE name (required for non-final steps)
 *   - select: Either a string (raw select list) or { columns?: {alias: expr}, sql?: string }
 *       - columns: alias -> expression map. `key === value` skips the alias;
 *         keys starting with `[sql]` emit raw values with no alias; `undefined` values are filtered out.
 *       - sql: optional raw column-list tail appended after columns
 *   - from: Source table/CTE (string)
 *   - joins: Either an array of { type, table, on? } or a string fallback
 *       - type: 'left', 'inner', 'cross', 'right', 'full'
 *       - on omitted for cross joins
 *   - where, having, qualify, 'order by', 'group by', limit: string clauses (limit may be a number)
 *
 *   Clauses are emitted in canonical SQL order regardless of input key order.
 *
 * RAW SHAPE:
 *   { name, query }
 *
 *   - name: CTE name
 *   - query: Entire CTE body as raw SQL, emitted verbatim
 *
 * Detection: a step is raw iff it has a top-level `query` key. Mixing raw and structured
 * keys within a single step throws.
 *
 * @param {Array<Object>} steps - Array of step objects defining the query structure
 * @returns {string} Generated SQL query
 */
const queryBuilder = (steps) => {
    const INDENT = 2;
    const pad = ' '.repeat(INDENT);

    // Re-indents a multi-line SQL fragment so that continuation lines
    // (lines after the first) have a consistent base indentation.
    // Preserves relative indentation within the fragment.
    const reindent = (sql, targetIndent) => {
        if (!sql.includes('\n')) return sql;
        const lines = sql.split('\n');
        const continuationLines = lines.slice(1).filter(l => l.trim());
        if (continuationLines.length === 0) return sql;
        const minIndent = Math.min(
            ...continuationLines.map(l => l.match(/^ */)[0].length)
        );
        const p = ' '.repeat(targetIndent);
        return lines[0] + '\n' + lines.slice(1)
            .map(l => l.trim() ? p + l.slice(minIndent) : '')
            .join('\n');
    };

    // Shifts an entire SQL block right by the given number of spaces.
    // Preserves relative indentation within the block.
    const indentBlock = (sql, spaces) => {
        const p = ' '.repeat(spaces);
        return sql.split('\n')
            .map(l => l.trim() ? p + l : '')
            .join('\n');
    };

    // Helper function to turn step.select.columns into SQL string
    const columnsToSQL = (columns) => {
        return Object.entries(columns)
            // exclude all columns that have been explicitly set to undefined
            .filter(([key, value]) => value !== undefined)
            .map(([key, value]) => {
                let entry;
                // if the key and value are the same, return the value as is (i.e. no alias)
                if (key === value) {
                    entry = value;
                // if the key starts with '[sql]', return the value as is (i.e. no alias)
                } else if (key.startsWith('[sql]')) {
                    entry = value;
                } else {
                    entry = `${value} as ${key}`;
                }
                return reindent(entry, INDENT);
            })
            .join(',\n' + pad);
    };

    // Renderer for the SELECT clause. Accepts a string (sugar for {sql: <string>})
    // or an object with columns and/or sql fields.
    const renderSelect = (value) => {
        const v = typeof value === 'string' ? { sql: value } : value;
        const hasColumns = v.columns !== undefined && Object.keys(v.columns).length > 0;
        const hasSql = typeof v.sql === 'string' && v.sql.length > 0;
        if (!hasColumns && !hasSql) {
            throw new Error('queryBuilder: select must include at least one of `columns` or `sql`');
        }
        const parts = [];
        if (hasColumns) parts.push(columnsToSQL(v.columns));
        if (hasSql) parts.push(reindent(v.sql, INDENT));
        return `select\n${pad}${parts.join(',\n' + pad)}`;
    };

    // Renderer for the JOINS clause. Accepts an array of {type, table, on}
    // entries (rendered in array order) or a string fallback.
    const renderJoins = (value) => {
        if (typeof value === 'string') {
            return reindent(value, 0);
        }
        return value
            .map(j => j.type === 'cross'
                ? `cross join\n${pad}${j.table}`
                : `${j.type} join\n${pad}${j.table} ${j.on}`)
            .join('\n');
    };

    // Renderer factory for inline string clauses (where, having, group by, ...).
    // Coerces non-strings (e.g. limit: 100) via String().
    const renderInline = (keyword) => (value) =>
        `${keyword}\n${pad}${reindent(String(value), INDENT)}`;

    // Registry of clause renderers. Declaration order is the canonical SQL order
    // — clauses are always emitted in this order regardless of input key order.
    const CLAUSE_RENDERERS = [
        { key: 'select',   render: renderSelect             },
        { key: 'from',     render: renderInline('from')     },
        { key: 'joins',    render: renderJoins              },
        { key: 'where',    render: renderInline('where')    },
        { key: 'group by', render: renderInline('group by') },
        { key: 'having',   render: renderInline('having')   },
        { key: 'qualify',  render: renderInline('qualify')  },
        { key: 'order by', render: renderInline('order by') },
        { key: 'limit',    render: renderInline('limit')    },
    ];

    const STRUCTURED_KEYS = new Set(['name', ...CLAUSE_RENDERERS.map(c => c.key)]);
    const RAW_KEYS = new Set(['name', 'query']);

    const validateStep = (step) => {
        if (!step || typeof step !== 'object' || Array.isArray(step)) {
            throw new Error(`queryBuilder: each step must be a non-null object, received: ${JSON.stringify(step)}`);
        }
        const isRaw = 'query' in step;
        const allowed = isRaw ? RAW_KEYS : STRUCTURED_KEYS;
        const allowedList = [...allowed].map(k => `\`${k}\``).join(', ');
        const stepLabel = step.name ? `\`${step.name}\`` : '<unnamed>';
        for (const key of Object.keys(step)) {
            if (!allowed.has(key)) {
                if (isRaw && STRUCTURED_KEYS.has(key) && key !== 'name') {
                    throw new Error(
                        `queryBuilder: step ${stepLabel} has both \`query\` (raw shape) and \`${key}\` (structured key). ` +
                        `Raw and structured shapes are mutually exclusive within a single step. ` +
                        `Allowed raw-shape keys: ${allowedList}.`
                    );
                }
                throw new Error(
                    `queryBuilder: unknown key \`${key}\` in ${isRaw ? 'raw' : 'structured'} step ${stepLabel}. ` +
                    `Allowed keys: ${allowedList}.`
                );
            }
        }
        if (isRaw) {
            if (typeof step.query !== 'string' || step.query.length === 0) {
                throw new Error(`queryBuilder: raw step ${stepLabel} requires a non-empty \`query\` string`);
            }
        } else {
            if (step.select === undefined) {
                throw new Error(`queryBuilder: structured step ${stepLabel} requires \`select\``);
            }
            if (step.from === undefined) {
                throw new Error(`queryBuilder: structured step ${stepLabel} requires \`from\``);
            }
        }
    };

    const renderStep = (step) => {
        validateStep(step);
        if ('query' in step) {
            // Raw shape: emit body verbatim, normalized to col 0 of the step.
            return reindent(step.query, 0);
        }
        return CLAUSE_RENDERERS
            .filter(c => step[c.key] !== undefined)
            .map(c => c.render(step[c.key]))
            .join('\n');
    };

    if (steps.length === 1) {
        return renderStep(steps[0]);
    }

    const ctes = steps.slice(0, -1).map(step => {
        const body = indentBlock(renderStep(step), INDENT);
        return `${step.name} as (\n${body}\n)`;
    });
    const lastStep = steps[steps.length - 1];
    return `with ${ctes.join(',\n')}\n${renderStep(lastStep)}`;
};

/**
 * Deep merge SQL configuration objects with special handling for nested objects and arrays.
 * 
 * Rules:
 * - Nested objects are merged recursively key by key
 * - Arrays with a "default" counterpart (e.g. excludedEvents + defaultExcludedEvents)
 *   are merged with mergeUniqueArrays, with user values taking precedence
 * - Arrays that are themselves a "default" version, or have no default counterpart,
 *   are overwritten by user input
 * - Default values are preserved unless explicitly overridden (including with undefined)
 * - Explicitly setting a value to undefined in inputConfig will override the default
 * - Date fields: after merging, specific date fields (listed in dateFields) are processed via processDate().
 *   Only fields that are defined in the result object are processed. String dates (e.g. '20260101',
 *   '2026-01-01') are converted to BigQuery SQL CAST expressions; SQL expressions (e.g. 'current_date()')
 *   are passed through unchanged.
 * 
 * @param {Object} defaultConfig - The default configuration object
 * @param {Object} inputConfig - The input configuration to merge (optional)
 * @returns {Object} The merged configuration object
 */
const mergeSQLConfigurations = (defaultConfig, inputConfig = {}) => {
    // If inputConfig is not an object, return defaultConfig
    if (!inputConfig || typeof inputConfig !== 'object' || Array.isArray(inputConfig)) {
        return defaultConfig;
    }

    // the merged configuration object
    const result = { ...defaultConfig };

    for (const key in inputConfig) {
        if (!inputConfig.hasOwnProperty(key)) continue;

        const inputValue = inputConfig[key];
        const defaultValue = defaultConfig[key];

        // If the key doesn't exist in default, just add it
        if (!(key in defaultConfig)) {
            result[key] = inputValue;
            continue;
        }

        // Handle arrays: overwrite with input value (default counterpart merging happens post-loop)
        if (Array.isArray(defaultValue) && Array.isArray(inputValue)) {
            result[key] = inputValue;
            continue;
        }

        // Handle nested objects: recursive merge
        if (
            defaultValue !== null &&
            inputValue !== null &&
            typeof defaultValue === 'object' &&
            typeof inputValue === 'object' &&
            !Array.isArray(defaultValue) &&
            !Array.isArray(inputValue)
        ) {
            result[key] = mergeSQLConfigurations(defaultValue, inputValue);
            continue;
        }

        // For all other cases (primitives, null, undefined), use input value
        result[key] = inputValue;
    }

    // Merge arrays with their default counterparts (e.g. excludedEvents + defaultExcludedEvents)
    // This runs regardless of whether the user provided the array in inputConfig
    for (const key in result) {
        if (!result.hasOwnProperty(key) || key.startsWith('default')) continue;
        const defaultKey = 'default' + key.charAt(0).toUpperCase() + key.slice(1);
        if (defaultKey in result && Array.isArray(result[key]) && Array.isArray(result[defaultKey])) {
            result[key] = mergeUniqueArrays(result[key], result[defaultKey]);
        }
    }

    // process configuration date fields
    // BigQuery SQL statements are excepted
    // string dates such as '20260101' or '2026-01-01' are processed
    const dateFields = [
        'preOperations.dateRangeStartFullRefresh', 
        'preOperations.dateRangeEnd', 
        'preOperations.incrementalStartOverride', 
        'preOperations.incrementalEndOverride', 
        'testConfig.dateRangeStart', 
        'testConfig.dateRangeEnd'
    ];

    for (const path of dateFields) {
        const parts = path.split('.');
        let obj = result;
        for (let i = 0; i < parts.length - 1; i++) {
            if (obj == null || typeof obj !== 'object') break;
            obj = obj[parts[i]];
        }
        if (obj != null && typeof obj === 'object') {
            const key = parts[parts.length - 1];
            const value = obj[key];
            if (value !== undefined) {
                obj[key] = processDate(value);
            }
        }
    }

    // support different formats for passing the sourceTable path
    const fixSourceTable = (sourceTable) => {
        if (isDataformTableReferenceObject(sourceTable)) {
            return sourceTable;
        }
        if (typeof sourceTable === 'string') {
            const tablePath = sourceTable.replace(/[`"']/g, '').trim();
            if (/^[a-zA-Z0-9-]+\.[a-zA-Z0-9_]+(\.[^\.]+)?$/.test(tablePath)) {
                const project = tablePath.split('.')[0];
                const dataset = tablePath.split('.')[1];
                return `\`${project}.${dataset}.events_*\``;
            }
        }
        throw new Error(`sourceTable must be a Dataform table reference or a string. Supported string formats include: '\`project.dataset.events_*\`', 'project.dataset', 'project.dataset.events_*', for example. Received: ${JSON.stringify(sourceTable)}`);
    };

    // process the sourceTable to support different formats
    if (result.sourceTable) {
        result.sourceTable = fixSourceTable(result.sourceTable);
    }

    // include the event parameters listed in the eventParamsToColumns array in excludedEventParams
    if (result.eventParamsToColumns && result.eventParamsToColumns.length > 0) {
        const promotedParameters = result.eventParamsToColumns
            .map(p => p.name)
            .filter(p => typeof p === 'string' && p.trim() !== '');
        result.excludedEventParams = mergeUniqueArrays(result.excludedEventParams, promotedParameters);
    }

    return result;
};

/**
 * Checks if a given object is a Dataform table reference object.
 *
 * A Dataform table reference object is expected to have the properties: 'name', and 'dataset'.
 *
 * @param {Object} obj - The object to check.
 * @returns {boolean} True if the object is a Dataform table reference, false otherwise.
 */
const isDataformTableReferenceObject = (obj) => {
    return obj &&
        typeof obj === 'object' &&
        Object.hasOwn(obj, 'name') &&
        // Dataform transforms the schema key to dataset key when using ctx.ref()
        (Object.hasOwn(obj, 'dataset') || Object.hasOwn(obj, 'schema'));
};


/**
 * Sets the Dataform context for a configuration object.
 *
 * This function updates the provided config object by resolving the `sourceTable` property. If the `sourceTable`
 * is a Dataform table reference object (with 'name', and 'dataset' properties), it uses `ctx.ref()` to
 * obtain the correct reference. Otherwise, it checks if `sourceTable` is a string in the format '`project.dataset.table`'.
 * If not, it throws an error. Finally, it sets the `self` and `incremental` properties on the config using
 * `ctx.self()` and `ctx.incremental()`, respectively.
 *
 * @param {Object} ctx - The Dataform context, contains methods for referencing tables and context information.
 * @param {Object} config - The configuration object to update with Dataform context and resolved table references.
 * @returns {Object} The updated configuration object with appropriate Dataform context set.
 * @throws {Error} If `sourceTable` is not a valid Dataform reference object or a correctly formatted string.
 */
const setDataformContext = (ctx, config) => {
    // if the sourceTable is a Dataform reference, use ctx.ref() to reference to it
    if (isDataformTableReferenceObject(config.sourceTable)) {
        config.sourceTable = ctx.ref(config.sourceTable);
    } else {
        // if the sourceTable is not a Dataform reference, it must be a string in the format '`project.dataset.table`'
        if (typeof config.sourceTable !== 'string' || !/^`[^\.]+\.[^\.]+\.[^\.]+`$/.test(config.sourceTable)) {
            throw new Error(`Failed to set Dataform context: config.sourceTable must be a Dataform table reference or a string in the format 'project.dataset.table'. Received: ${JSON.stringify(config.sourceTable)}`);
        }
    }

    // resolve Dataform refs in enrichments[].source the same way as sourceTable
    if (Array.isArray(config.enrichments)) {
        config.enrichments = config.enrichments.map(e => {
            if (isDataformTableReferenceObject(e.source)) {
                return { ...e, source: ctx.ref(e.source) };
            }
            return e;
        });
    }

    config.self = ctx.self();
    config.incremental = ctx.incremental();

    return config;
};

/**
 * Deep merge Dataform table configuration objects.
 * 
 * Rules:
 * - Nested objects are merged recursively key by key (e.g. setting bigquery.partitionBy
 *   does not overwrite the whole bigquery object)
 * - Array fields are overwritten by user input by default
 * - The 'tags' array is an exception: default tags are preserved and user input is concatenated
 * - Default values are preserved unless the user input explicitly overrides them
 * 
 * @param {Object} defaultConfig - The default Dataform table configuration object
 * @param {Object} inputConfig - The user-provided table configuration to merge (optional)
 * @returns {Object} The merged Dataform table configuration object
 */
const mergeDataformTableConfigurations = (defaultConfig, inputConfig = {}) => {
    if (!inputConfig || typeof inputConfig !== 'object' || Array.isArray(inputConfig)) {
        return defaultConfig;
    }

    // Array keys where default values should be preserved and user input concatenated
    const concatenateArrayKeys = ['tags'];

    const deepMerge = (defaultObj, inputObj) => {
        const result = { ...defaultObj };

        for (const key in inputObj) {
            if (!inputObj.hasOwnProperty(key)) continue;

            const inputValue = inputObj[key];
            const defaultValue = defaultObj[key];

            // If the key doesn't exist in default, just add it
            if (!(key in defaultObj)) {
                result[key] = inputValue;
                continue;
            }

            // Handle arrays
            if (Array.isArray(defaultValue) && Array.isArray(inputValue)) {
                // Concatenate and deduplicate for specified keys, overwrite for all others
                result[key] = concatenateArrayKeys.includes(key)
                    ? mergeUniqueArrays(defaultValue, inputValue)
                    : inputValue;
                continue;
            }

            // Handle nested objects: recursive merge
            if (
                defaultValue !== null &&
                inputValue !== null &&
                typeof defaultValue === 'object' &&
                typeof inputValue === 'object' &&
                !Array.isArray(defaultValue) &&
                !Array.isArray(inputValue)
            ) {
                result[key] = deepMerge(defaultValue, inputValue);
                continue;
            }

            // For all other cases (primitives, null, undefined), use input value
            result[key] = inputValue;
        }

        return result;
    };

    return deepMerge(defaultConfig, inputConfig);
};

/**
 * Builds a queryBuilder `select.columns` fragment that passes through every source column
 * not already covered by an explicit columns object.
 *
 * A source column is considered "covered" — and skipped from pass-throughs — when it appears as:
 *   - a KEY in `explicitColumns` (a transform, package promotion, or undefined-valued exclusion
 *     sentinel like `{ event_dimensions: undefined }`), OR
 *   - a VALUE in `explicitColumns` (a bare source-column identifier referenced by a value-side
 *     rename, e.g. `{ user_traffic_source: 'traffic_source' }` covers 'traffic_source').
 *
 * Values that are SQL expressions, function calls, or non-strings never count as coverage —
 * they reference the source column internally but the column itself is still available as a
 * pass-through. (`.includes()` compares by strict equality, so 'extract(datetime from ...)'
 * never matches a bare column name.)
 *
 * @param {Object} explicitColumns - A queryBuilder step's explicit `select.columns` entries.
 * @param {Iterable<string>} sourceColumns - Column names available on the source schema.
 * @returns {Object} A map of `{ column: column }` entries for every source column not covered.
 *
 * @example
 *   buildPassThroughs(
 *     { event_name: 'event_name', user_traffic_source: 'traffic_source' },
 *     ['event_name', 'traffic_source', 'device', 'geo']
 *   );
 *   // → { device: 'device', geo: 'geo' }
 */
const buildPassThroughs = (explicitColumns, sourceColumns) => {
    const explicitKeys = Object.keys(explicitColumns);
    const explicitValues = Object.values(explicitColumns);
    const passThroughs = {};
    for (const column of sourceColumns) {
        if (!explicitKeys.includes(column) && !explicitValues.includes(column)) {
            passThroughs[column] = column;
        }
    }
    return passThroughs;
};


/**
 * Builds the per-enrichment CTE definitions, JOIN clauses, and column-name mappings for the
 * declarative `enrichments` feature. Routes row-level and item-level entries through
 * separate output channels so the caller can attach them to different downstream CTEs.
 *
 * Pure config-to-data mapping. No knowledge of downstream CTEs or specific table modules —
 * intended to be called by any table module that exposes an `enrichments` config field.
 *
 * Encapsulates one generation-time throw:
 *   - Same-level enrichment-vs-enrichment column collisions (two row-level enrichments or
 *     two item-level enrichments targeting the same column). Cross-level same-name is allowed —
 *     the two columns target structurally distinct slots (e.g. `enhanced_events.<col>` vs
 *     `items[].<col>`).
 *
 * @param {Array<Object>} enrichments - Validated enrichment entries. Each entry has fields:
 *   { name, source, joinKey, columns, level?, dedupe? }. `level` is 'row' (default) or 'item'.
 *   'row' means one row of the enclosing table per join match; 'item' targets a nested array
 *   (currently only the GA4 items[] array).
 * @returns {Object} A struct with four fields:
 *   - `steps` — array of queryBuilder source-CTE step definitions (one `enrich_<name>` per
 *     entry, regardless of level — all source CTEs go to the top of the pipeline).
 *   - `row` — { joins, columns, columnNames } for row-level enrichments. Caller attaches
 *     `joins` to the row-grained downstream CTE (e.g. `enhanced_events`) and spreads `columns`
 *     into that CTE's `select.columns`.
 *   - `item` — { joins, columns, columnNames } for item-level enrichments. Caller attaches
 *     `joins` to the item-grained downstream CTE (e.g. `items_rebuilt`) and folds `columns`
 *     into that CTE's struct construction.
 *   - `columnOwner` — map of `{ <column>: { i, name, level } }` recording which enrichment
 *     owns each column. The `level` field distinguishes cross-level same-name entries.
 *
 * @throws {Error} If two same-level enrichments target the same column name (with both
 *   enrichment names and the conflicting column in the error message).
 *
 * @example
 *   const { steps, row, item } = buildEnrichments(config.enrichments);
 *   // row.joins → attach to enhanced_events; row.columns → spread into enhanced_events
 *   // item.joins → attach to items_rebuilt; item.columns → fold into items struct
 */
const buildEnrichments = (enrichments) => {
    const steps = [];
    const channels = {
        row: { joins: [], columns: {}, columnNames: new Set() },
        item: { joins: [], columns: {}, columnNames: new Set() },
    };
    const columnOwner = {};

    for (const [i, e] of (enrichments ?? []).entries()) {
        const level = e.level ?? 'row';
        const channel = channels[level];
        const joinKeys = Array.isArray(e.joinKey) ? e.joinKey : [e.joinKey];
        const cteName = `enrich_${e.name}`;

        // Source CTE selects joinKey columns plus the requested columns. key === value
        // shape skips the alias clause in queryBuilder's columnsToSQL.
        const cteCols = {};
        for (const k of joinKeys) cteCols[k] = k;
        for (const c of e.columns) cteCols[c] = c;
        const sourceStep = { name: cteName, select: { columns: cteCols }, from: e.source };
        // Opt-in dedupe: which row wins is non-deterministic — users with strict needs
        // pre-aggregate in their source SQL.
        if (e.dedupe) {
            sourceStep.qualify = `row_number() over (partition by ${joinKeys.join(', ')}) = 1`;
        }
        steps.push(sourceStep);

        channel.joins.push({ type: 'left', table: cteName, on: `using(${joinKeys.join(', ')})` });

        for (const c of e.columns) {
            // Same-level collision throw. Cross-level same-name is allowed because the two
            // columns target structurally distinct output slots (event_data vs items[]).
            if (channel.columnNames.has(c)) {
                const owner = columnOwner[c];
                throw new Error(
                    `config.enrichments[${i}] (name: '${e.name}') and config.enrichments[${owner.i}] ` +
                    `(name: '${owner.name}') both target column '${c}' at level '${level}'. ` +
                    `Two enrichments cannot write the same column at the same level; rename one in source SQL or pick a different name.`
                );
            }
            channel.columns[c] = `${cteName}.${c}`;
            channel.columnNames.add(c);
            // columnOwner is keyed by column name; if the same name appears at different
            // levels, the second-writer entry wins, but we record level so diagnostics
            // distinguish them. Same-level collisions throw above before reaching here.
            columnOwner[c] = { i, name: e.name, level };
        }
    }

    return { steps, row: channels.row, item: channels.item, columnOwner };
};


/**
 * Builds a qualified pass-through fragment for spreading into a downstream SELECT's
 * `select.columns`. For each column in `step.select.columns` not already in `alreadyCovered`,
 * emits an entry of the form `{ <col>: '<step.name>.<col>' }`.
 *
 * Columns whose values in `step.select.columns` are `undefined` (the user-exclusion sentinel
 * shape from getExcludedColumns) are skipped. Names in `alreadyCovered` that don't exist in
 * `step.select.columns` are silently ignored — the loop only iterates `step.select.columns`,
 * so unknown names cause no harm. This is the safety property that lets callers pass
 * "everything that might collide" without pre-filtering.
 *
 * @param {Object} step - A queryBuilder step with a `name` and `select.columns` object.
 * @param {Iterable<string>} alreadyCovered - Column names already mapped elsewhere in the
 *   downstream SELECT, plus any internal-only columns the downstream SELECT shouldn't re-emit.
 * @returns {Object} A map of `{ <col>: '<step.name>.<col>' }` entries.
 *
 * @example
 *   buildQualifiedPassThroughs(eventDataStep, ['event_date', 'session_id', 'entrances']);
 *   // → { event_name: 'event_data.event_name', user_pseudo_id: 'event_data.user_pseudo_id', ... }
 */
const buildQualifiedPassThroughs = (step, alreadyCovered) => {
    const covered = new Set(alreadyCovered);
    const passThroughs = {};
    for (const [col, expr] of Object.entries(step.select.columns)) {
        if (expr === undefined) continue;
        if (covered.has(col)) continue;
        passThroughs[col] = `${step.name}.${col}`;
    }
    return passThroughs;
};


/**
 * Processes a date input string and returns a corresponding SQL date casting expression,
 * or passes through BigQuery SQL statements as-is.
 *
 * Supported formats:
 * - YYYYMMDD (e.g. "20260101"): returns a SQL CAST using the 'YYYYMMDD' date format.
 * - YYYY-MM-DD (e.g. "2026-01-01"): returns a SQL CAST using the 'YYYY-MM-DD' date format.
 * - BigQuery SQL statement: input that starts with an identifier (letters and underscores only)
 *   followed by parentheses (e.g. "current_date()", "current_date()-1", "date(2026, 1, 1)") is
 *   returned unchanged.
 *
 * @param {string} dateInput - The input date string or SQL expression to process.
 * @returns {string} A SQL date casting expression or the input SQL statement unchanged.
 * @throws {Error} If the input is not a non-empty string, or if the format is not supported.
 *
 * @example
 * processDate("20260101")           // returns "cast('20260101' as date format 'YYYYMMDD')"
 * processDate("2026-01-01")         // returns "cast('2026-01-01' as date format 'YYYY-MM-DD')"
 * processDate("current_date()")     // returns "current_date()"
 * processDate("date(2026, 1, 1)")   // returns "date(2026, 1, 1)"
 */
const processDate = (dateInput) => {
    if (typeof dateInput !== 'string') {
        throw new Error(`processDate: dateInput must be a string. Received type: ${typeof dateInput}, value: ${JSON.stringify(dateInput)}`);
    }
    if (dateInput.trim() === '') {
        throw new Error('processDate: dateInput cannot be an empty string.');
    }

    // If input is a string in the format YYYYMMDD (e.g., "20260101"), return as cast(... as date format 'YYYYMMDD')
    if (/^\d{8}$/.test(dateInput.trim())) {
        return `cast('${dateInput}' as date format 'YYYYMMDD')`;
    }

    // If input is a string in the format YYYY-MM-DD (e.g., "2026-01-01"), return as cast(... as date format 'YYYY-MM-DD')
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateInput.trim())) {
        return `cast('${dateInput}' as date format 'YYYY-MM-DD')`;
    }

    // If input is a BigQuery SQL statement (identifier of letters/underscores followed by parens, e.g. "current_date()-1", "date(2026, 1, 1)"), return it as is
    if (/^[a-zA-Z_]+\s*\(/.test(dateInput.trim())) {
        return dateInput;
    }

    throw new Error(`processDate: Unsupported date input format: ${JSON.stringify(dateInput)}. Expected formats are: YYYYMMDD, YYYY-MM-DD, or BigQuery SQL statement.`);
};

/**
 * Extracts the dataset name from a sourceTable configuration value.
 *
 * Supports both Dataform table reference objects (with 'dataset' or 'schema' property)
 * and backtick-quoted strings in the format '`project.dataset.table`'.
 *
 * @param {Object|string} sourceTable - A Dataform table reference object or a backtick-quoted string.
 * @returns {string} The dataset name.
 * @throws {Error} If the dataset name cannot be extracted from the provided value.
 */
const getDatasetName = (sourceTable) => {
    if (isDataformTableReferenceObject(sourceTable)) {
        return sourceTable.dataset || sourceTable.schema;
    }
    if (typeof sourceTable === 'string' && /^`[^\.]+\.[^\.]+\.[^\.]+`$/.test(sourceTable)) {
        return sourceTable.split('.')[1];
    }
    throw new Error(`Unable to extract the dataset name from sourceTable, received: ${JSON.stringify(sourceTable)}`);
};

module.exports = {
    mergeUniqueArrays,
    mergeSQLConfigurations,
    mergeDataformTableConfigurations,
    queryBuilder,
    isDataformTableReferenceObject,
    setDataformContext,
    buildPassThroughs,
    buildEnrichments,
    buildQualifiedPassThroughs,
    processDate,
    getDatasetName
};