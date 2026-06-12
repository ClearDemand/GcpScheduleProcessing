// ----------------------------------------------------------------------------
//  RetailScape Cloud SQL (Postgres) resources (ES module)
//
//  A SECOND pg Pool, separate from aurora_resources.js (which connects to the
//  match-library Cloud SQL via the `pmtAurora` secret). This one connects to
//  the RetailScape Postgres using the `rsAurora` secret
//  (process.env.secretsManager.rsAurora -> e.g. "dev/retailscape/psql").
//
//  The rsAurora secret is a FLAT JSON shape (NOT nested under `aurora` like
//  pmtAurora). On GCP the gcp_host / gcp_password fields are used — mirrors
//  retailscape-baas/retailscape/lib/db/init_db.py getRetailscapePostgresConfig.
//  `schema` (e.g. "dev") is the Postgres search_path; tenant_feature_v2 lives
//  there (see rs_etl_master ... {env}.tenant_feature_v2).
// ----------------------------------------------------------------------------
import pkg from 'pg';
import { getSecretAsJson } from './secret_manager_resources.js';

const { Pool } = pkg;

let pool;
let schema;

// Create the connection pool once on startup.
export async function init() {
    const secretName = JSON.parse(process.env.secretsManager).rsAurora;
    const cfg = await getSecretAsJson(secretName);

    schema = cfg.schema || process.env.dbENV;

    pool = new Pool({
        host: cfg.gcp_host,
        user: cfg.username,
        password: cfg.gcp_password,
        database: cfg.dbName,
        port: cfg.port,
        max: 10,
        idleTimeoutMillis: 10000,
        connectionTimeoutMillis: 100000,
        ssl: { rejectUnauthorized: false },
        options: `-c search_path=${schema}`
    });

    await pool.query('SELECT NOW()');
    console.log(`RetailScape Postgres connected (search_path=${schema})`);
}

// tenants with the match_library feature enabled, each with its per-tenant
// export_config (from feature_config->'export_config'). Source of truth for
// which tenants the export runs for.
export async function getMatchLibraryTenants() {
    const query = `
        SELECT tenant_code, feature_config->'export_config' as export_config
        FROM tenant_feature_v2
        WHERE lower(feature_name) = lower('match_library')
          AND is_enabled = true
    `;
    const res = await pool.query(query);
    return res.rows
        .filter(r => r.tenant_code)
        .map(r => ({ tenant_code: r.tenant_code, export_config: r.export_config }));
}
