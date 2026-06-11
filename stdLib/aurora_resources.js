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
export async function getMatchesByPartition(company_code, base_source_store, comp_source_store, columns, maxCaptureDate) {
    try {
        let catalogQuery = '';
        let catColumQuery = '';
        if (maxCaptureDate) {
            catalogQuery += ` JOIN catalog_${company_code}_${company_code}_${company_code}_${maxCaptureDate} cat ON mat.base_sku = cat.sku`;
            catColumQuery += ', cat.segment';
        }

        // Copy so we don't mutate the shared exportColumns array across partitions.
        let cols = [...columns];
        if (company_code === 'chewy') {
            cols.push('base_custom_attributes', 'comp_custom_attributes');
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
