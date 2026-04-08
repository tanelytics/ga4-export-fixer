/**
 * Dataform API Client Wrapper for Integration Tests
 *
 * Handles workspace lifecycle, compilation, workflow invocations,
 * and action result querying via the Dataform API.
 */

const { DataformClient } = require('@google-cloud/dataform').v1beta1;

/**
 * Create a Dataform API client.
 * @returns {DataformClient}
 */
const createClient = () => new DataformClient();

/**
 * Create a temporary workspace and pull the latest from the default branch.
 *
 * @param {DataformClient} client
 * @param {string} repositoryPath - e.g. projects/proj/locations/loc/repositories/repo
 * @param {string} workspaceId - unique workspace name (e.g. 'integration-test-1712592000')
 * @returns {Promise<string>} full workspace resource path
 */
const createTestWorkspace = async (client, repositoryPath, workspaceId) => {
    const [workspace] = await client.createWorkspace({
        parent: repositoryPath,
        workspace: {},
        workspaceId,
    });

    const workspacePath = workspace.name;

    // Pull latest from the default branch
    await client.pullGitCommits({
        name: workspacePath,
        author: {
            name: 'Integration Test',
            emailAddress: 'integration-test@ga4-export-fixer.dev',
        },
    });

    return workspacePath;
};

/**
 * Update the ga4-export-fixer version in the workspace's package.json.
 *
 * @param {DataformClient} client
 * @param {string} workspacePath - full workspace resource path
 * @param {string} packageName - npm package name (e.g. 'ga4-export-fixer')
 * @param {string} version - version to install (e.g. '0.4.5-dev.0')
 */
const updatePackageVersion = async (client, workspacePath, packageName, version) => {
    // Read existing package.json
    const [readResponse] = await client.readFile({
        workspace: workspacePath,
        path: 'package.json',
    });

    const packageJson = JSON.parse(readResponse.fileContents.toString('utf8'));

    // Update version
    if (packageJson.dependencies && packageJson.dependencies[packageName] !== undefined) {
        packageJson.dependencies[packageName] = version;
    } else {
        // Ensure dependencies object exists
        packageJson.dependencies = packageJson.dependencies || {};
        packageJson.dependencies[packageName] = version;
    }

    // Write updated package.json
    await client.writeFile({
        workspace: workspacePath,
        path: 'package.json',
        contents: Buffer.from(JSON.stringify(packageJson, null, 4)),
    });

    // Install packages
    await client.installNpmPackages({ workspace: workspacePath });
};

/**
 * Compile the workspace and return the compilation result name.
 * Throws if there are compilation errors.
 *
 * @param {DataformClient} client
 * @param {string} repositoryPath
 * @param {string} workspacePath
 * @returns {Promise<string>} compilation result resource name
 */
const compileWorkspace = async (client, repositoryPath, workspacePath) => {
    const [compilationResult] = await client.createCompilationResult({
        parent: repositoryPath,
        compilationResult: {
            workspace: workspacePath,
        },
    });

    const errors = compilationResult.compilationErrors || [];
    if (errors.length > 0) {
        const errorMessages = errors.map(e =>
            `${e.path || 'unknown'}${e.actionTarget ? ` (${e.actionTarget.name})` : ''}: ${e.message}`
        ).join('\n  ');
        throw new Error(`Compilation failed with ${errors.length} error(s):\n  ${errorMessages}`);
    }

    return compilationResult.name;
};

/**
 * Discover actions in a compilation result that match a given tag.
 * Returns table targets (dataset + table name).
 *
 * @param {DataformClient} client
 * @param {string} compilationResultName
 * @param {string} tag
 * @returns {Promise<Array<{dataset: string, name: string}>>}
 */
const discoverTaggedActions = async (client, compilationResultName, tag) => {
    const actions = [];

    const iterable = client.queryCompilationResultActionsAsync({
        name: compilationResultName,
    });

    for await (const action of iterable) {
        if (!action.target) continue;

        // Match by Dataform tags (top-level) or BigQuery labels
        const actionTags = action.tags || [];
        const bigqueryLabels = action.relation?.relationDescriptor?.bigqueryLabels || {};
        const hasTag = actionTags.includes(tag) || bigqueryLabels[tag] !== undefined;
        const isTable = action.relation != null;

        if (hasTag && isTable) {
            actions.push({
                dataset: action.target.schema || action.target.database,
                name: action.target.name,
            });
        }
    }

    return actions;
};

/**
 * Create a workflow invocation, poll until terminal state, and return the result.
 *
 * @param {DataformClient} client
 * @param {string} repositoryPath
 * @param {string} compilationResultName
 * @param {Object} options
 * @param {string} options.tag - tag to filter actions
 * @param {boolean} options.fullRefresh - whether to fully refresh incremental tables
 * @param {number} options.timeoutMs - max time to wait
 * @returns {Promise<{state: string, name: string, elapsedMs: number}>}
 */
const runWorkflowInvocation = async (client, repositoryPath, compilationResultName, options) => {
    const { tag, fullRefresh = false, timeoutMs = 600000 } = options;

    const [invocation] = await client.createWorkflowInvocation({
        parent: repositoryPath,
        workflowInvocation: {
            compilationResult: compilationResultName,
            invocationConfig: {
                includedTags: [tag],
                fullyRefreshIncrementalTablesEnabled: fullRefresh,
                transitiveDependenciesIncluded: true,
            },
        },
    });

    const invocationName = invocation.name;
    const startTime = Date.now();

    // Poll with exponential backoff: 5s -> 10s -> 20s -> 30s (capped)
    let delay = 5000;
    const maxDelay = 30000;

    // Terminal states from the Dataform API
    const terminalStates = ['SUCCEEDED', 'FAILED', 'CANCELLED'];
    // State enum values (numeric)
    const stateMap = {
        0: 'STATE_UNSPECIFIED',
        1: 'RUNNING',
        2: 'SUCCEEDED',
        3: 'CANCELLED',
        4: 'FAILED',
        5: 'CANCELING',
    };

    while (true) {
        const elapsed = Date.now() - startTime;
        if (elapsed > timeoutMs) {
            // Attempt to cancel
            try {
                await client.cancelWorkflowInvocation({ name: invocationName });
            } catch (_) { /* best effort */ }
            return { state: 'TIMEOUT', name: invocationName, elapsedMs: elapsed };
        }

        await new Promise(resolve => setTimeout(resolve, delay));
        delay = Math.min(delay * 2, maxDelay);

        const [current] = await client.getWorkflowInvocation({ name: invocationName });
        const stateValue = typeof current.state === 'number' ? stateMap[current.state] : String(current.state);

        if (terminalStates.includes(stateValue)) {
            return {
                state: stateValue,
                name: invocationName,
                elapsedMs: Date.now() - startTime,
            };
        }
    }
};

/**
 * Query individual action results from a workflow invocation.
 *
 * @param {DataformClient} client
 * @param {string} invocationName
 * @returns {Promise<Array<{target: string, state: string, failureReason: string|null}>>}
 */
const getActionResults = async (client, invocationName) => {
    const results = [];

    const stateMap = {
        0: 'STATE_UNSPECIFIED',
        1: 'PENDING',
        2: 'RUNNING',
        3: 'SKIPPED',
        4: 'DISABLED',
        5: 'SUCCEEDED',
        6: 'CANCELLED',
        7: 'FAILED',
    };

    const iterable = client.queryWorkflowInvocationActionsAsync({
        name: invocationName,
    });

    for await (const action of iterable) {
        const stateValue = typeof action.state === 'number' ? stateMap[action.state] : String(action.state);
        results.push({
            target: action.target ? `${action.target.schema}.${action.target.name}` : 'unknown',
            state: stateValue,
            failureReason: action.failureReason || null,
        });
    }

    return results;
};

/**
 * Delete a workspace (for cleanup).
 *
 * @param {DataformClient} client
 * @param {string} workspacePath
 */
const deleteTestWorkspace = async (client, workspacePath) => {
    await client.deleteWorkspace({ name: workspacePath });
};

module.exports = {
    createClient,
    createTestWorkspace,
    updatePackageVersion,
    compileWorkspace,
    discoverTaggedActions,
    runWorkflowInvocation,
    getActionResults,
    deleteTestWorkspace,
};
