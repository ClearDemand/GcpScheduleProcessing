// ----------------------------------------------------------------------------
//  Cloud SQL (Postgres) resources (ES module)
//
//  Connection mirrors matchlibrary-baas/src/lib/db/aurora.js: a pg Pool whose
//  credentials come from GCP Secret Manager (secret name in
//  process.env.secretsManager.pmtAurora), with search_path = process.env.dbENV.
//
//  Ports the queries match_library_export.js used against AWS Aurora — the
//  table shapes are identical on the migrated Cloud SQL instance.
// ----------------------------------------------------------------------------
import pkg from 'pg';
import moment from 'moment';
import { getSecretAsJson } from './secret_manager_resources.js';
import { DEFAULT_CATALOG_ATTRIBUTE_MAPPING } from './catalog_attribute_mapping.js';

const { Pool } = pkg;

let pool;

// Create the connection pool once on startup.
export async function init() {
    const secretName = JSON.parse(process.env.secretsManager).pmtAurora;
    const dbConfig = await getSecretAsJson(secretName);

    pool = new Pool({
        host: dbConfig.aurora.host,
        user: dbConfig.aurora.username,
        password: dbConfig.aurora.password,
        database: dbConfig.aurora.database,
        port: dbConfig.aurora.port,
        max: 50,
        idleTimeoutMillis: 10000,
        connectionTimeoutMillis: 100000,
        ssl: { rejectUnauthorized: false },
        options: `-c search_path=${process.env.dbENV}`
    });

    await pool.query('SELECT NOW()');
    console.log(`Cloud SQL connected (search_path=${process.env.dbENV})`);
}

export async function runQuery(text, values) {
    const res = await pool.query(text, values);
    return res.rows;
}

// Ported from PmtScheduleProcessing aurora_resources.getMatchesByPartition.
export async function getMatchesByPartition(company_code, base_source_store, comp_source_store, columns, maxCaptureDate, additional_columns = []) {
    try {
        let catalogQuery = '';
        let catColumQuery = '';
        if (maxCaptureDate) {
            catalogQuery += ` JOIN catalog_${company_code}_${company_code}_${company_code}_${maxCaptureDate} cat ON mat.base_sku = cat.sku`;
            catColumQuery += ', cat.segment';
        }

        // Copy so we don't mutate the shared exportColumns array across partitions.
        let cols = [...columns];
        if (additional_columns.length>0) {
            cols.push(...additional_columns);
        }

        cols = [...new Set(cols)];

        let query = `
            SELECT ${cols.map(column => `mat.${column}`).join(',')} ${catColumQuery}
            FROM matches_${company_code}_${base_source_store} mat
            ${catalogQuery}
            WHERE mat.match_status = 'product_found'
            AND mat.deleted_date is null AND mat.match in ('exact', 'equivalent', 'reference','similar')
            AND comp_source_store = '${comp_source_store}';
        `;
        query = query.replace(/\n|\t/g, '');

        const res = await pool.query(query);
        return res.rows;
    } catch (err) {
        console.log(`getMatchesByPartition err for matches_${company_code}_${base_source_store}_${comp_source_store}: ${err.message}`);
        return [];
    }
}

// Ported from PmtScheduleProcessing aurora_resources.getLatestCatalogDate.
export async function getLatestCatalogDate(company_code, source_store) {
    try {
        const query = `SELECT cast(MAX(capture_date) as text) FROM catalog_${company_code}_${source_store}`;
        const res = await pool.query(query);
        return res.rows[0] || false;
    } catch (error) {
        console.log(`getLatestCatalogDate err: ${error.message}`);
        return false;
    }
}

// Ported from PmtScheduleProcessing tenant_mapping_resources.getTenantMappings.
export async function getTenantMappings(tenantCode) {
    try {
        const rows = await runQuery('SELECT * FROM tenant_mapping WHERE tenant_code = $1', [tenantCode]);
        const mappings = (rows || [])[0] || {};

        return {
            category: mappings.category || 'category',
            category_display: mappings.category_display || 'Category',
            subcategory: mappings.subcategory || 'Subscategory',
            subcategory_display: mappings.subcategory_display || 'Subscategory',
            sub_subcategory: mappings.sub_subcategory || 'Sub Subcategory',
            sub_subcategory_display: mappings.sub_subcategory_display || 'Sub Subcategory',
            sub_sub_subcategory: mappings.sub_sub_subcategory || 'sub_sub_subcategory',
            sub_sub_subcategory_display: mappings.sub_sub_subcategory_display || 'Sub Sub Subcategory',
            sub_sub_sub_subcategory: mappings.sub_sub_sub_subcategory || 'sub_sub_sub_subcategory',
            sub_sub_sub_subcategory_display: mappings.sub_sub_sub_subcategory_display || 'Sub Sub Sub Subcategory'
        };
    } catch (error) {
        console.log(`${moment().format()} - getTenantMappings error: ${error.message}`);
        return 'n/a';
    }
}

// ============================================================================
//  Catalog Sync resources — ported from PmtScheduleProcessing aurora_resources.
//
//  The original used matchlibraryPool.connect() + connection.query(cb); here we
//  use the shared pg Pool via runQuery()/pool.query promises. Table shapes are
//  identical on the migrated Cloud SQL instance, and table names are left bare
//  so the search_path (= process.env.dbENV, set in init()) resolves them — the
//  same convention getMatchesByPartition above relies on.
// ============================================================================

// Column names + types for a table, from information_schema (resolved within the
// dbENV schema so it matches what the pool's search_path uses).
export async function getSchema(table) {
    try {
        const rows = await runQuery(
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema = $2`,
            [table, process.env.dbENV]
        );
        return rows || [];
    } catch (error) {
        console.log(`getSchema err for ${table}: ${error.message}`);
        return [];
    }
}

// ============================================================================
//  Catalog-sync plan — driven by DEFAULT_CATALOG_ATTRIBUTE_MAPPING
//  (stdLib/catalog_attribute_mapping.js), overridable per tenant via
//  `catalog_attribute_mapping_overrides` on the company-code doc. Replaces the
//  previous hardcoded catalog-column -> base_* switch, the CATALOG_JSON_COLUMNS
//  const and the exclusion-list schema discovery: the mapping is now the
//  single authority on WHICH base_* columns sync and HOW their value is
//  derived. buildCatalogSyncPlan() compiles the entries that apply to a tenant
//  + target table into the SQL fragments fetch/updateMatchWithCatalog consume.
// ============================================================================

// information_schema data_types treated as numeric for diff comparisons —
// replaces the old hardcoded `catalogColumn === 'list_price'` special case.
const NUMERIC_DATA_TYPES = new Set(['numeric', 'integer', 'bigint', 'smallint', 'double precision', 'real', 'money']);

// Ingestion-time transforms with no sync-time meaning (the sync only copies
// catalog values onto existing matches; it never builds new rows).
const SYNC_UNSUPPORTED_TRANSFORMS = new Set(['json_to_string', 'json_extract_base_upc', 'prefix']);

// Drops mapping entries whose `enabled_by_flag` is off on this tenant's company-code doc.
function resolveMappingEntry(entry, client) {
    if (entry.enabled_by_flag && !client[entry.enabled_by_flag]) return null;
    return entry;
}

// Compiles one mapping entry into the SQL fragments the sync queries need, or
// null when it can't sync from this tenant's catalog table (no source column,
// ingestion-only transform, or the only source is the sku join key).
function buildColumnSpec(target, entry, catalogTypes) {
    if (entry.transform === 'json_extract') {
        if (!catalogTypes.has(entry.source)) return null;
        // Safely extract from JSON: validate format first (starts with { or [), return NULL on invalid JSON
        const jsonExpr = `CASE WHEN cat.${entry.source} IS NOT NULL AND cat.${entry.source} <> '' AND (cat.${entry.source} LIKE '{%' OR cat.${entry.source} LIKE '[%') THEN COALESCE(cat.${entry.source}::jsonb->>'${entry.key}', NULL) ELSE NULL END`;
        return {
            target,
            valueExpr: jsonExpr,
            nullCheckExpr: `${jsonExpr} IS NOT NULL AND ${jsonExpr} != ''`,
            isNumeric: false
        };
    }

    if (entry.transform === 'text_to_jsonb') {
        if (!catalogTypes.has(entry.source)) return null;
        const jsonbExpr = `CASE WHEN cat.${entry.source} IS NOT NULL AND cat.${entry.source} <> '' THEN cat.${entry.source}::jsonb ELSE NULL END`;
        return {
            target,
            valueExpr: jsonbExpr,
            nullCheckExpr: `${jsonbExpr} IS NOT NULL`,
            isNumeric: false
        };
    }

    if (entry.transform && entry.transform !== 'first_token') {
        const reason = SYNC_UNSUPPORTED_TRANSFORMS.has(entry.transform) ? 'ingestion-only' : 'unknown';
        console.log(`buildColumnSpec: ${target} uses ${reason} transform '${entry.transform}' - skipped`);
        return null;
    }

    // sku is the join key (mss.base_sku = res.sku), so syncing from it is a
    // no-op; only sources that exist on this tenant's catalog table count.
    const sources = (entry.sources || [entry.source])
        .filter(col => col && col !== 'sku' && catalogTypes.has(col));
    if (!sources.length) return null;

    const isNumeric = sources.every(col => NUMERIC_DATA_TYPES.has(catalogTypes.get(col)));
    const multiSource = sources.length > 1;
    const exprs = sources.map(col => {
        const numericCol = NUMERIC_DATA_TYPES.has(catalogTypes.get(col));
        let expr = `cat.${col}`;
        if (numericCol && !isNumeric) expr = `(${expr})::text`;
        // inside a coalesce chain '' must become NULL so the next source gets a chance
        if (!numericCol && multiSource) expr = `NULLIF(${expr}, '')`;
        if (entry.transform === 'first_token') expr = `split_part(${expr}, ' ', 1)`;
        return expr;
    });
    const valueExpr = multiSource ? `COALESCE(${exprs.join(', ')})` : exprs[0];

    return { target, valueExpr, isNumeric };
}

// Compiles the effective mapping into the per-column sync plan for one tenant
// and target table (matches_<cc>_<cc>_<cc> or tpvr_<cc>): only entries whose
// target column exists on the table and whose flags are on for this client.
// The effective mapping is DEFAULT_CATALOG_ATTRIBUTE_MAPPING with any
// per-tenant `catalog_attribute_mapping_overrides` on the company-code doc
// merged on top, column by column (an override entry replaces the default
// entry for that column wholesale; columns without an override keep the
// default). Most tenants have no overrides and just get the default.
//   client - the tenant's company-code doc (source of the behaviour flags and
//            any catalog_attribute_mapping_overrides)
export async function buildCatalogSyncPlan(catalogTable, targetTable, client) {
    try {
        const mapping = { ...DEFAULT_CATALOG_ATTRIBUTE_MAPPING, ...(client.catalog_attribute_mapping_overrides || {}) };

        const catalogTypes = new Map((await getSchema(catalogTable)).map(r => [r.column_name, r.data_type]));
        const targetCols = new Set((await getSchema(targetTable)).map(r => r.column_name));

        // base_total_size = size * pack_size rides along on whichever of these
        // two columns' own UPDATE pass runs (see recompute_total_size in the
        // mapping doc) — only when this tenant's catalog has both source
        // columns and the target table has base_total_size to write into.
        const canRecomputeTotalSize = catalogTypes.has('size') && catalogTypes.has('pack_size') && targetCols.has('base_total_size');

        const plan = [];
        for (const [target, rawEntry] of Object.entries(mapping)) {
            const entry = resolveMappingEntry(rawEntry, client);
            if (!entry || !targetCols.has(target)) continue;

            const spec = buildColumnSpec(target, entry, catalogTypes);
            if (!spec) continue;

            if (entry.recompute_total_size && canRecomputeTotalSize) spec.recomputeTotalSize = true;
            plan.push(spec);
        }
        return plan;
    } catch (error) {
        console.log(`buildCatalogSyncPlan err (${catalogTable} -> ${targetTable}): ${error.message}`);
        return [];
    }
}

// Latest capture_date present in the tenant's catalog table.
export async function getLatestCatalogCaptureDate(catalog_base_partition, company_code) {
    try {
        const query = `SELECT cast(MAX(capture_date) as text) as capture_date FROM ${catalog_base_partition} where company_code = '${company_code}'`;
        const rows = await runQuery(query);
        return (rows && rows[0] && rows[0].capture_date) || false;
    } catch (error) {
        console.log(`getLatestCatalogCaptureDate err: ${error.message}`);
        return false;
    }
}

// capture_date currently recorded in catalog_version (the last synced date).
export async function getCatalogVersionDate(company_code) {
    try {
        const query = `SELECT cast(capture_date as text) as capture_date FROM catalog_version WHERE company_code = '${company_code}';`;
        const rows = await runQuery(query);
        if (rows && rows[0] && rows[0].capture_date) return rows[0].capture_date;
        return (rows && rows[0]) || false;
    } catch (error) {
        console.log(`getCatalogVersionDate err: ${error.message}`);
        return false;
    }
}

// Whether a catalog_version row already exists for this tenant.
async function getCatalogVersion(company_code) {
    try {
        const rows = await runQuery(`SELECT * from catalog_version where company_code = $$${company_code}$$`);
        return !!(rows && rows.length);
    } catch (error) {
        console.log(`getCatalogVersion err: ${error.message}`);
        return false;
    }
}

async function updateCatalogVersion(company_code, capture_date) {
    try {
        await runQuery(`UPDATE catalog_version set capture_date = $$${capture_date}$$ where company_code = $$${company_code}$$`);
        return true;
    } catch (error) {
        console.log(`updateCatalogVersion err: ${error.message}`);
        return false;
    }
}

// Upserts the tenant's synced capture_date into catalog_version.
export async function insertToCatalogVersion(company_code, capture_date) {
    try {
        const exists = await getCatalogVersion(company_code);
        if (exists) {
            return await updateCatalogVersion(company_code, capture_date);
        }
        await runQuery(`INSERT INTO catalog_version (company_code, capture_date) VALUES ($$${company_code}$$, $$${capture_date}$$);`);
        return true;
    } catch (error) {
        console.log(`insertToCatalogVersion err: ${error.message}`);
        return false;
    }
}

// Updates matches whose base attribute differs from the latest catalog value,
// returning the affected rows. `spec` is one buildCatalogSyncPlan() entry —
// the mapping doc decides the value expression; this only assembles the query.
export async function updateMatchWithCatalog(targetTable, catalog_base_partition, maxCatalogDate, spec, company_code, options = {}) {
    try {
        const {
            catalogActiveOnly = false  // only sync from catalog rows where is_active (was: staterbros)
        } = options;

        const current_time = new Date().toISOString();

        // multiply-variant base_total_size recompute rides along on the
        // base_size / base_pack_size updates (see buildCatalogSyncPlan).
        let totalSizeCondition = '';
        let companionSelect = '';
        if (spec.recomputeTotalSize) {
            if (spec.target === 'base_pack_size') {
                companionSelect = ', cat.size';
                totalSizeCondition = ` ,base_total_size=(res.sync_value::numeric) * (res.size::numeric)`;
            } else if (spec.target === 'base_size') {
                companionSelect = `, split_part(cat.pack_size, ' ', 1 ) as pack_size`;
                totalSizeCondition = ` ,base_total_size=(res.pack_size::numeric) * (res.sync_value::numeric)`;
            }
        }

        // Null-safe on purpose: a null/empty catalog value that differs from the
        // target IS a discrepancy and gets written (clearing the match attribute).
        // '' and NULL are treated as the same "empty" state to avoid churn.
        // Cast to text before LOWER() to handle jsonb columns (text::text is no-op).
        const matchDiffCondition = spec.isNumeric
            ? `mss.${spec.target} IS DISTINCT FROM res.sync_value::float8`
            : `NULLIF(LOWER(mss.${spec.target}::text), '') IS DISTINCT FROM NULLIF(lower(res.sync_value::text), '')`;

        let query = `
        WITH filtered_catalog AS (
            SELECT DISTINCT
                ${spec.valueExpr} as sync_value,
                cat.sku,
                cat.is_active
                ${companionSelect}
            FROM
                ${catalog_base_partition} cat
            WHERE
                cat.capture_date = '${maxCatalogDate}'
                and cat.company_code = '${company_code}'
                ${catalogActiveOnly ? ` AND cat.is_active` : ''}
        )
        UPDATE ${targetTable} mss
        SET ${spec.target} = ${spec.target === 'base_custom_attributes' ? 'res.sync_value::jsonb' : 'res.sync_value'}
        ${totalSizeCondition}
            , internal_notes = 'match_update_by_catalog_processor: ${current_time}'
        FROM filtered_catalog res
        WHERE mss.base_sku = res.sku
            AND mss.company_code = '${company_code}'
            AND mss.deleted_by is null AND mss.deleted_date is null AND mss.match_status = 'product_found'
            AND ${matchDiffCondition}
            RETURNING mss.*;
        `;
        query = query.replace(/\n|\t/g, '');

        const rows = await runQuery(query);
        return rows || false;
    } catch (error) {
        console.log(`updateMatchWithCatalog err: ${error.message}`);
        return false;
    }
}

// Returns matches whose base attribute differs from the latest catalog value
// (the discrepancy report rows), without mutating anything. `spec` is one
// buildCatalogSyncPlan() entry, same as updateMatchWithCatalog.
export async function fetchMatchDifferentWithCatalog(targetTable, catalog_base_partition, maxCatalogDate, spec, company_code, options = {}) {
    try {
        const { catalogActiveOnly = false } = options;

        // Null-safe on purpose — see updateMatchWithCatalog's matchDiffCondition.
        // Cast to text before LOWER() to handle jsonb columns (text::text is no-op).
        const matchDiffCondition = spec.isNumeric
            ? `mss.${spec.target} IS DISTINCT FROM res.sync_value::float8`
            : `NULLIF(LOWER(mss.${spec.target}::text), '') IS DISTINCT FROM NULLIF(lower(res.sync_value::text), '')`;

        let query = `
        WITH filtered_catalog AS (
            SELECT DISTINCT
                ${spec.valueExpr} as sync_value,
                cat.sku,
                is_active
            FROM
                ${catalog_base_partition} cat
            WHERE
                cat.capture_date = '${maxCatalogDate}'
                and cat.company_code = '${company_code}'
                ${catalogActiveOnly ? ` AND cat.is_active` : ''}
        )
        select
            mss.match_id,
            mss.base_sku,
            mss.company_code,
            mss.company_code_base_source_store ,
            mss.comp_sku,
            mss.comp_source_store,
            mss.base_category,
            mss.base_subcategory,
            mss.base_sub_subcategory,
            mss.base_sub_sub_subcategory,
            mss.base_sub_sub_sub_subcategory,
            mss.base_url,
            mss.base_upc,
            mss.base_uom,
            mss.base_size,
            mss.base_brandtype,
            mss.base_brand,
            mss.base_title,
            mss.base_pack_size,
            mss.base_total_size,
            mss.normalized_base_uom,
            mss.normalized_comp_size,
            mss.normalized_comp_total_size,
            mss.segment,
            mss.comp_upc,
            mss.comp_brand,
            mss.base_mfr_part_number,
            mss.comp_mfr_part_number,
            mss.active,
            mss.match,
            mss.model_used,
            '${spec.target} is not matching' as update_reason,
            res.sync_value as updated_value
            FROM ${targetTable} mss
            INNER JOIN filtered_catalog res ON mss.base_sku = res.sku
            WHERE
            mss.company_code = '${company_code}'
            AND mss.deleted_by is null AND mss.deleted_date is null AND mss.match_status = 'product_found'
            AND ${matchDiffCondition};`;
        query = query.replace(/\n|\t/g, '');

        const rows = await runQuery(query);
        return rows || false;
    } catch (error) {
        console.log(`fetchMatchDifferentWithCatalog err: ${error.message}`);
        return false;
    }
}

// Catalog-active flag sync for the tpvr_<cc> table. Mirrors the catalog row's
// own is_active value onto is_catalog_active for every base_sku that HAS a row
// in the latest catalog capture (bidirectional: flips to false when the
// catalog marks it inactive, back to true when it's active again). base_skus
// with no row at all in the latest capture are left untouched — absence alone
// doesn't imply inactive. Only rows whose flag would actually change are
// written, and the changed rows are RETURNed for reporting.
//
//   targetTable         - tpvr_<cc> or a matches base table (must have the
//                         is_catalog_active column; skipped with a log if not)
//   catalog_base_partition / maxCatalogDate / company_code - the latest catalog
export async function syncCatalogActiveFlag(targetTable, catalog_base_partition, maxCatalogDate, company_code) {
    try {
        // Guard: the column is provisioned on tpvr_<cc> today but may not yet
        // exist on the matches tables. Skip (don't crash the tenant) until the
        // DDL lands — the sync activates automatically once it does.
        const targetCols = await getSchema(targetTable);
        if (!targetCols.some(c => c.column_name === 'is_catalog_active')) {
            console.log(`syncCatalogActiveFlag: ${targetTable} has no is_catalog_active column - skipping`);
            return [];
        }

        let query = `
        WITH latest_catalog AS (
            SELECT lower(sku) AS sku, bool_or(is_active) AS is_active
            FROM ${catalog_base_partition}
            WHERE capture_date = '${maxCatalogDate}'
              AND company_code = '${company_code}'
            GROUP BY lower(sku)
        ),
        target_state AS (
            SELECT t2.match_id, COALESCE(lc.is_active, false) AS in_catalog
            FROM ${targetTable} t2
            LEFT JOIN latest_catalog lc ON lower(t2.base_sku) = lc.sku
            WHERE t2.company_code = '${company_code}'
              AND t2.deleted_date IS NULL
        )
        UPDATE ${targetTable} t
        SET is_catalog_active = ts.in_catalog
        FROM target_state ts
        WHERE t.match_id = ts.match_id
          AND t.is_catalog_active IS DISTINCT FROM ts.in_catalog
        RETURNING t.match_id, t.base_sku, t.comp_sku, t.comp_source_store, ts.in_catalog AS is_catalog_active;
        `;
        query = query.replace(/\n|\t/g, '');

        const rows = await runQuery(query);
        return rows || [];
    } catch (error) {
        console.log(`syncCatalogActiveFlag err for ${targetTable}: ${error.message}`);
        return [];
    }
}

function formatSqlLiteral(val) {
    if (val === null) return 'NULL';                 // explicit null = clear the column
    if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
    if (typeof val === 'string') {
        const escaped = val.replace(/'/g, "''");
        // If it looks like a date, cast as timestamp
        return /^\d{4}-\d{2}-\d{2}T/.test(val) ? `'${escaped}'::timestamp` : `'${escaped}'`;
    }
    return val;
}

// Cap on rows per UPDATE ... FROM (VALUES ...) statement, so a large sync run
// (e.g. a mass UPC change) can't build one unbounded SQL string — each chunk
// stays a small, fast statement instead.
const ATTRIBUTE_UPDATE_CHUNK_SIZE = 5;

// Per-match attribute UPDATE, batched into set-based UPDATE ... FROM (VALUES ...)
// statements (ATTRIBUTE_UPDATE_CHUNK_SIZE rows per statement) instead of one
// UPDATE per match_id. Assumes every row sets the same columns — true for the
// only caller (multi-UPC resolution in catalog_sync.js, which always sets
// base_upc + internal_notes).
export async function updateAttributes(updates, companyCode, targetTable = `matches_${companyCode}_${companyCode}_${companyCode}`) {
    try {
        const entries = Object.entries(updates || {});
        if (!entries.length) return;

        const columns = Object.keys(entries[0][1]);

        for (let i = 0; i < entries.length; i += ATTRIBUTE_UPDATE_CHUNK_SIZE) {
            const chunk = entries.slice(i, i + ATTRIBUTE_UPDATE_CHUNK_SIZE);
            const valuesList = chunk
                .map(([match_id, values]) => `('${match_id}', ${columns.map(col => formatSqlLiteral(values[col])).join(', ')})`)
                .join(', ');

            const query = `
                UPDATE ${targetTable} mss
                SET ${columns.map(col => `${col} = v.${col}`).join(', ')}
                FROM (VALUES ${valuesList}) AS v(match_id, ${columns.join(', ')})
                WHERE mss.match_id::text = v.match_id;
            `;

            await runQuery(query);
        }
    } catch (error) {
        console.error(`updateAttributes err: ${error.message}`);
        return [];
    }
}
