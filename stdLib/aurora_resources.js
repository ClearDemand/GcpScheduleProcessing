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
import { randomUUID } from 'crypto';
import { getSecretAsJson } from './secret_manager_resources.js';

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
        console.log(`getMatchesByPartition err for matches_${company_code}_${base_source_store}: ${err.message}`);
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

// JSON-backed catalog columns that are not top-level columns in the catalog
// table. Structure: { company_code: { catalogColumn: { selectExpr, nullCheckExpr } } }
const CATALOG_JSON_COLUMNS = {
    ctc: {
        sub_sub_sub_subcategory: {
            selectExpr:    `cat.additional_attributes::json->>'sub_sub_sub_subcategory' as sub_sub_sub_subcategory`,
            nullCheckExpr: `cat.additional_attributes::json->>'sub_sub_sub_subcategory' is not null and cat.additional_attributes::json->>'sub_sub_sub_subcategory' != ''`
        }
    }
};

// Resolves the matches base column that a given catalog column maps to.
function baseMatchesColumnFor(catalogColumn) {
    switch (catalogColumn) {
        case 'product_url':         return 'base_url';
        case 'image_url':           return 'base_img';
        case 'brand_type':          return 'base_brandtype';
        case 'product_title':       return 'base_title';
        case 'product_description': return 'base_description';
        case 'segment':             return 'segment';
        case 'list_price':          return 'base_price';
        default:                    return 'base_' + catalogColumn;
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
// returning the affected rows. Mirrors the AWS implementation verbatim except
// for the connection style.
export async function updateMatchWithCatalog(matches_base_partition, catalog_base_partition, maxCatalogDate, catalogColumn, company_code, totalSizeToBeUpdated = false) {
    try {
        const current_time = new Date().toISOString();
        const baseMatchesColumn = baseMatchesColumnFor(catalogColumn);

        let totalSizeCondition = '';
        if (totalSizeToBeUpdated) {
            totalSizeCondition = ` ,base_total_size=(res.pack_size::numeric) * (res.size::numeric)`;
        }
        let standardizedBaseUpcCondition = '';
        if (company_code == 'bjs') {
            standardizedBaseUpcCondition = ` ,standardized_base_upc=res.additional_attributes::json->>'base_converted_upc'`;
        }

        const jsonOverride = CATALOG_JSON_COLUMNS[company_code]?.[catalogColumn];
        const catalogSelectExpr = jsonOverride?.selectExpr ?? (
            catalogColumn === 'pack_size' ? `split_part(cat.${catalogColumn}, ' ', 1) as pack_size` : `cat.${catalogColumn}`
        );
        const isNumericCatalogColumn = catalogColumn === 'list_price';
        const catalogNullCheckExpr = jsonOverride?.nullCheckExpr ?? (
            isNumericCatalogColumn ? `cat.${catalogColumn} is not null` : `cat.${catalogColumn} is not null and cat.${catalogColumn} != ''`
        );
        const matchDiffCondition = isNumericCatalogColumn
            ? `(mss.${baseMatchesColumn} is null OR mss.${baseMatchesColumn} != res.${catalogColumn}::float8)`
            : `(mss.${baseMatchesColumn} is null OR mss.${baseMatchesColumn} = '' OR LOWER(mss.${baseMatchesColumn}) != lower(res.${catalogColumn}))`;

        let query = `
        WITH filtered_catalog AS (
            SELECT DISTINCT
                ${catalogSelectExpr},
                cat.sku,
                cat.is_active
                ${catalogColumn == 'pack_size' ? ', cat.size' : (catalogColumn == 'size' ? `, split_part(cat.pack_size, ' ', 1 ) as pack_size` : '')}
                ${company_code == 'bjs' && catalogColumn == 'upc' ? ', cat.additional_attributes' : ''}
            FROM
                ${catalog_base_partition} cat
            WHERE
                cat.capture_date = '${maxCatalogDate}'
                and cat.company_code = '${company_code}'
                ${company_code == 'staterbros' ? ` AND cat.is_active` : ''}
                and ${catalogNullCheckExpr}
        )
        UPDATE ${matches_base_partition} mss
        SET ${baseMatchesColumn} = res.${catalogColumn}
        ${catalogColumn == 'upc' ? standardizedBaseUpcCondition : ''}
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
// (the discrepancy report rows), without mutating anything.
export async function fetchMatchDifferentWithCatalog(matches_base_partition, catalog_base_partition, maxCatalogDate, catalogColumn, company_code) {
    try {
        const baseMatchesColumn = baseMatchesColumnFor(catalogColumn);

        const jsonOverride = CATALOG_JSON_COLUMNS[company_code]?.[catalogColumn];
        const catalogSelectExpr = jsonOverride?.selectExpr ?? (
            catalogColumn === 'pack_size' ? `split_part(cat.${catalogColumn}, ' ', 1) as pack_size` : `cat.${catalogColumn}`
        );
        const isNumericCatalogColumn = catalogColumn === 'list_price';
        const catalogNullCheckExpr = jsonOverride?.nullCheckExpr ?? (
            isNumericCatalogColumn ? `cat.${catalogColumn} is not null` : `cat.${catalogColumn} is not null and cat.${catalogColumn} != ''`
        );
        const matchDiffCondition = isNumericCatalogColumn
            ? `(mss.${baseMatchesColumn} is null OR mss.${baseMatchesColumn} != res.${catalogColumn}::float8)`
            : `(mss.${baseMatchesColumn} is null OR mss.${baseMatchesColumn} = '' OR LOWER(mss.${baseMatchesColumn}) != lower(res.${catalogColumn}))`;

        let query = `
        WITH filtered_catalog AS (
            SELECT DISTINCT
                ${catalogSelectExpr},
                cat.sku,
                is_active
            FROM
                ${catalog_base_partition} cat
            WHERE
                cat.capture_date = '${maxCatalogDate}'
                and cat.company_code = '${company_code}'
                ${company_code == 'staterbros' ? ` AND cat.is_active` : ''}
                and ${catalogNullCheckExpr}
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
            mss.base_title,
            mss.base_pack_size,
            mss.base_total_size,
            mss.normalized_base_uom,
            mss.normalized_comp_size,
            mss.normalized_comp_total_size,
            mss.segment,
            mss.comp_upc,
            mss.model_used,
            '${baseMatchesColumn} is not matching' as update_reason,
            res.${catalogColumn} as updated_value
            FROM ${matches_base_partition} mss
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

// Active UOM normalization map keyed by `${company_code}-${ORIGINAL_UOM}`.
export async function getUomNormalizationData() {
    try {
        const query = `
            SELECT UPPER(original_uom) AS original_uom, UPPER(normalized_uom) AS normalized_uom,
                    LOWER(company_code) AS company_code
                    FROM uom_normalized_data
                    WHERE active = true
        `.replace(/\n|\t/g, '');
        const rows = await runQuery(query);
        const uomNormalizationData = {};
        for (const row of rows) {
            uomNormalizationData[`${row.company_code}-${row.original_uom}`] = row.normalized_uom;
        }
        return uomNormalizationData;
    } catch (error) {
        console.log(`${moment().format()} - getUomNormalizationData error: ${error.message}`);
        return {};
    }
}

// Active UOM size-conversion map keyed by `${company_code}-${BASE_UOM}-${COMP_UOM}`.
export async function getUomSizeConversionData() {
    try {
        const query = `
            SELECT UPPER(normalized_base_uom) AS normalized_base_uom, UPPER(normalized_comp_uom) AS normalized_comp_uom, LOWER(company_code) AS company_code,
                   conversion_factor
            FROM uom_size_conversion_data
            WHERE active = true
        `.replace(/\n|\t/g, '');
        const rows = await runQuery(query);
        const uomSizeConversionData = {};
        for (const row of rows) {
            uomSizeConversionData[`${row.company_code}-${row.normalized_base_uom}-${row.normalized_comp_uom}`] = row.conversion_factor;
        }
        return uomSizeConversionData;
    } catch (error) {
        console.log(`${moment().format()} - getUomSizeConversionData error: ${error.message}`);
        return {};
    }
}

// Bulk-updates normalized UOM/size columns via a single VALUES-join UPDATE.
export async function updateNormalizedAttributes(updates, companyCode) {
    try {
        if (!updates || Object.keys(updates).length === 0) return '';

        const allCols = new Set();
        Object.values(updates).forEach(obj => {
            Object.keys(obj).forEach(col => allCols.add(col));
        });

        const orderedCols = ['match_id', ...Array.from(allCols)];
        const valueRows = [];

        for (const [match_id, values] of Object.entries(updates)) {
            const row = [`'${match_id}'`, ...orderedCols.slice(1).map(col => {
                let val = values[col];
                if (val === undefined || val === null) return 'NULL';
                if (typeof val === 'string') {
                    val = val.replace(/'/g, "''");
                    return `'${val}'`;
                }
                return val;
            })];
            valueRows.push(`(${row.join(', ')})`);
        }

        const setClause = orderedCols.slice(1)
            .map(col => `${col} = c.${col}`)
            .join(', ');

        const query = `
          UPDATE matches_${companyCode}_${companyCode}_${companyCode} AS m
          SET ${setClause}
          FROM (
            VALUES
              ${valueRows.join(',\n        ')}
          ) AS c(${orderedCols.join(', ')})
          WHERE m.match_id = c.match_id;
        `;

        return await runQuery(query);
    } catch (error) {
        console.log(`updateNormalizedAttributes err: ${error.message}`);
        return [];
    }
}

// Per-match attribute UPDATEs (one statement per match_id), batched together.
export async function updateAttributes(updates, companyCode) {
    try {
        if (!updates || Object.keys(updates).length === 0) return '';

        const updateQueries = [];

        for (const [match_id, values] of Object.entries(updates)) {
            const setClauses = [];

            for (const [col, val] of Object.entries(values)) {
                if (val === null || val === undefined) continue;

                let formattedVal;
                if (typeof val === 'boolean') {
                    formattedVal = val ? 'TRUE' : 'FALSE';
                } else if (typeof val === 'string') {
                    const escaped = val.replace(/'/g, "''");
                    // If it looks like a date, cast as timestamp
                    if (/^\d{4}-\d{2}-\d{2}T/.test(val)) {
                        formattedVal = `'${escaped}'::timestamp`;
                    } else {
                        formattedVal = `'${escaped}'`;
                    }
                } else {
                    formattedVal = val;
                }

                setClauses.push(`${col} = ${formattedVal}`);
            }

            if (setClauses.length === 0) continue;

            const query = `
              UPDATE matches_${companyCode}_${companyCode}_${companyCode}
              SET ${setClauses.join(', ')}
              WHERE match_id = '${match_id}';
            `;
            updateQueries.push(query);
        }

        const finalQuery = updateQueries.join('\n');
        return await runQuery(finalQuery);
    } catch (error) {
        console.error(`updateAttributes err: ${error.message}`);
        return [];
    }
}

// ============================================================================
//  Match Update Processor resources — ported from PmtScheduleProcessing
//  poll.js / aurora_resources.js. Used by jobs/match_update_processor.js.
// ============================================================================

// Single-row, dynamic-column update for one match — used instead of
// updateNormalizedAttributes/updateAttributes above, which union columns
// across a whole batch and would null out any column a given row doesn't set.
// Different matches in the same sync batch change different subsets of the
// ~20 possible comp_* keys, so each match needs its own SET clause.
export async function updateMatchAttributes(companyCode, baseSourceStore, compSourceStore, matchId, modifyValues) {
    const cols = Object.keys(modifyValues);
    if (cols.length === 0) return null;

    const setClause = cols.map((c, i) => `${c} = $${i + 2}`).join(', ');
    const table = `matches_${companyCode}_${baseSourceStore}`;
    const rows = await runQuery(
        `UPDATE ${table} SET ${setClause} WHERE match_id = $1 RETURNING *`,
        [matchId, ...cols.map(c => modifyValues[c])]
    );
    return rows[0] || null;
}

// Brand-type lookup for a competitor brand, from brand_master. Ported from
// PmtScheduleProcessing aurora_resources.getCompBrandType.
export async function getCompBrandType(sourceStore, brand) {
    try {
        const rows = await runQuery(
            `SELECT brand_type FROM brand_master WHERE lower(source_store) = lower($1) AND lower(brand) = lower($2)`,
            [sourceStore, brand]
        );
        return (rows && rows[0]) ? rows[0].brand_type : false;
    } catch (error) {
        console.log(`getCompBrandType err: ${error.message}`);
        return false;
    }
}

// Bulk audit-trail insert into matches_audit_trail. Ported from
// PmtScheduleProcessing aurora_resources.insertIntoAuditRecords — old code
// used pg-promise's ColumnSet/insert helper and the `uuid4` npm package for
// audit_id; this uses Node's built-in crypto.randomUUID() (no new dependency)
// and a manually-built parameterized multi-row INSERT (avoiding the old
// code's manual quote-escaping entirely).
export async function insertIntoAuditRecords(companyCode, auditRows, reason, updateAction, updatedBy = 'gcp-scheduler', source = 'gcp-scheduler') {
    if (!auditRows || auditRows.length === 0) return null;

    const columns = ['audit_id', 'company_code', 'match_id', 'updated_by', 'updated_timestamp',
        'updated_reason', 'update_action', 'old_value', 'new_value', 'base_sku', 'comp_sku', 'comp_source_store', 'source'];

    const valueRows = [];
    const values = [];
    for (const row of auditRows) {
        const rowValues = [
            randomUUID(), companyCode, row.match_id ?? null, updatedBy, new Date().toISOString(),
            reason, updateAction,
            row.prev_value ? JSON.stringify(row.prev_value) : null,
            row.new_value ? JSON.stringify(row.new_value) : null,
            row.base_sku ?? null, row.comp_sku ?? null, row.comp_source_store ?? null, source
        ];
        const placeholders = rowValues.map((_, i) => `$${values.length + i + 1}`);
        valueRows.push(`(${placeholders.join(', ')})`);
        values.push(...rowValues);
    }

    try {
        return await runQuery(
            `INSERT INTO matches_audit_trail (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`,
            values
        );
    } catch (error) {
        console.log(`insertIntoAuditRecords err: ${error.message}`);
        return null;
    }
}
