// ----------------------------------------------------------------------------
//  Athena resources (ES module) — cross-cloud read of the pricing-parquet
//  dataset. No GCP/BigQuery equivalent exists yet, so this Cloud Run Job
//  queries Athena directly using the same cross-cloud AWS credentials as
//  dynamo_resources.js (secretsManager.awsCrossCloud).
//
//  Ported from PmtScheduleProcessing/stdLib/athena_resources.js
//  #getPricingParquetRowsForMatches, minus the `athena-express` dependency
//  (not ESM/v3-friendly) — StartQueryExecution/poll/GetQueryResults done
//  explicitly below with @aws-sdk/client-athena.
// ----------------------------------------------------------------------------
import {
    AthenaClient,
    StartQueryExecutionCommand,
    GetQueryExecutionCommand,
    GetQueryResultsCommand
} from '@aws-sdk/client-athena';
import { getSecretAsJson } from './secret_manager_resources.js';

const OUTPUT_LOCATION = 's3://bungee.productmatching/athenaqueries/';
const PRICING_DB = 'bungeepricingreports';
const POLL_INTERVAL_MS = 1000;
const POLL_TIMEOUT_MS = 5 * 60 * 1000;

let client;

export async function init() {
    const secretName = JSON.parse(process.env.secretsManager).awsCrossCloud;
    const cfg = await getSecretAsJson(secretName);

    client = new AthenaClient({
        region: cfg.region || 'us-east-1',
        credentials: { accessKeyId: cfg.AWS_ACCESS_KEY_ID, secretAccessKey: cfg.AWS_SECRET_ACCESS_KEY }
    });
    console.log(`Athena (AWS cross-cloud) client ready (region=${cfg.region || 'us-east-1'})`);
}

async function runAthenaQuery(sql, database) {
    const { QueryExecutionId } = await client.send(new StartQueryExecutionCommand({
        QueryString: sql,
        QueryExecutionContext: { Database: database },
        ResultConfiguration: { OutputLocation: OUTPUT_LOCATION }
    }));

    const startedAt = Date.now();
    let state;
    do {
        if (Date.now() - startedAt > POLL_TIMEOUT_MS) {
            throw new Error(`Athena query ${QueryExecutionId} timed out after ${POLL_TIMEOUT_MS}ms`);
        }
        await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
        const { QueryExecution } = await client.send(new GetQueryExecutionCommand({ QueryExecutionId }));
        state = QueryExecution.Status.State;
        if (state === 'FAILED' || state === 'CANCELLED') {
            throw new Error(`Athena query ${QueryExecutionId} ${state}: ${QueryExecution.Status.StateChangeReason}`);
        }
    } while (state !== 'SUCCEEDED');

    return fetchAllRows(QueryExecutionId);
}

async function fetchAllRows(QueryExecutionId) {
    const rows = [];
    let columns = null;
    let NextToken;
    let firstPage = true;

    do {
        const resp = await client.send(new GetQueryResultsCommand({ QueryExecutionId, NextToken }));
        if (!columns) columns = resp.ResultSet.ResultSetMetadata.ColumnInfo.map(c => c.Name);

        // Athena repeats the header as row 0, but only on the first page.
        const dataRows = firstPage ? resp.ResultSet.Rows.slice(1) : resp.ResultSet.Rows;
        for (const row of dataRows) {
            const obj = {};
            row.Data.forEach((d, i) => { obj[columns[i]] = d.VarCharValue ?? null; });
            rows.push(obj);
        }
        NextToken = resp.NextToken;
        firstPage = false;
    } while (NextToken);

    return rows;
}

const escapeSql = v => `${v}`.replace(/'/g, "''");

const UPC_MATCHES_DB = 'ml_stack_prod_matches';

// ML-suggested UPC auto-approval matches for one client/day. Ported from
// PmtScheduleProcessing/stdLib/athena_resources.js#getUpcMatchesToProcess
// (same SQL, minus the athena-express wrapper -- see this file's header).
// The legacy call passed an arbitrary 'temp' database label to athena-express
// (the query's FROM clause is already fully schema-qualified, so it barely
// mattered); this passes the actual schema name instead, for clarity.
export async function getUpcMatchesToProcess(companyCode, loadDate) {
    try {
        return await runAthenaQuery(
            `SELECT * FROM ${UPC_MATCHES_DB}.auto_approval_matches
             WHERE company_code = '${escapeSql(companyCode)}'
             AND model_used != 'automation-reactivated-matches'
             AND load_date = '${escapeSql(loadDate)}'`,
            UPC_MATCHES_DB
        );
    } catch (err) {
        console.log(`getUpcMatchesToProcess err: ${err.message}`);
        return [];
    }
}

// Authoritative pricing-parquet rows for a batch of matches. Two-step query:
// (1) find the latest year/month/day partition for the tenant, (2) pull the
// requested attribute columns for exactly the (base_sku, comp_source_store,
// comp_sku) tuples in `flatMatches`. Ported 2-step SQL shape from the old
// athena-express-based version.
export async function getPricingParquetRowsForMatches(company_code, pricingParquetKeyMap, flatMatches, keysToUpdate, pricingParquetCustomAttributesKeys) {
    try {
        if (!flatMatches || flatMatches.length === 0) return [];

        const requiredKeys = Array.isArray(keysToUpdate) && keysToUpdate.length > 0
            ? keysToUpdate : Object.keys(pricingParquetKeyMap || {});

        const attributeSet = new Set();
        for (const key of requiredKeys) {
            if (key === 'comp_custom_attributes' && pricingParquetCustomAttributesKeys) {
                for (const v of Object.values(pricingParquetCustomAttributesKeys)) {
                    if (v?.val) attributeSet.add(v.val);
                }
                continue;
            }
            const v = pricingParquetKeyMap?.[key];
            if (v?.val) attributeSet.add(v.val);
        }
        if (attributeSet.size === 0) return [];

        const attributeList = [...attributeSet];
        attributeList.push(`json_extract_scalar(extradetails, '$.all_pack_sizes') AS all_pack_sizes`);

        const tuples = flatMatches
            .filter(m => m?.base_sku && m?.comp_source_store && m?.comp_sku)
            .map(m => `('${escapeSql(m.base_sku)}','${escapeSql(m.comp_source_store)}','${escapeSql(m.comp_sku)}')`);
        if (tuples.length === 0) return [];

        const partitionRows = await runAthenaQuery(
            `SELECT year, month, day FROM "${PRICING_DB}"."pricing_parquet$partitions"
             WHERE client = '${escapeSql(company_code)}' ORDER BY year DESC, month DESC, day DESC LIMIT 1`,
            PRICING_DB
        );
        if (!partitionRows.length) return [];
        const { year, month, day } = partitionRows[0];
        if (!year || !month || !day) return [];

        return await runAthenaQuery(
            `SELECT DISTINCT base_sku, source_store AS comp_source_store, sku AS comp_sku, ${attributeList.join(', ')}
             FROM "${PRICING_DB}"."pricing_parquet"
             WHERE client = '${escapeSql(company_code)}' AND year = '${year}' AND month = '${month}' AND day = '${day}'
             AND (base_sku, source_store, sku) IN (${tuples.join(',')})`,
            PRICING_DB
        );
    } catch (err) {
        console.log(`getPricingParquetRowsForMatches err: ${err.message}`);
        return [];
    }
}
