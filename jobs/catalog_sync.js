// ----------------------------------------------------------------------------
//  Job: Catalog Sync (GCP)
//
//  Port of PmtScheduleProcessing/scripts/catalog_sync_processor.js. For each
//  tenant with the `catalog_syncup_process` flag set in the Firestore
//  company-code library, it brings the tenant's match rows in sync with the
//  latest catalog capture:
//
//    - derives the columns to sync from the Firestore catalog attribute mapping
//      (catalog_sync_config/attribute_mapping, provisioned by
//      scripts/provision_catalog_attribute_mapping.js) — the mapping declares
//      each base_* target column and how its value comes out of the catalog
//      (coalesce chains, JSON extraction, transforms), and only entries whose
//      source/target columns exist on this tenant's tables are run;
//    - reconciles the tpvr-only `is_catalog_active` flag: false for base_skus no
//      longer in the latest catalog, true for those present again (bidirectional);
//    - records the synced capture_date in catalog_version and uploads a report.
//
//  All the per-tenant behaviour that the original keyed on company_code strings
//  (ctc UPC resolution + JSON columns, staterbros active-only catalog) is now
//  driven by flags on the Firestore company-code doc — row-level flags via
//  deriveTenantFlags, column-level flags via the mapping doc's
//  `enabled_by_flag` references — there are no hardcoded company codes here.
//  The originally thrive-only base_total_size = pack_size * size recompute now
//  applies to every tenant whose catalog has both columns (mapping doc entry).
//  The bjs-specific logic in the AWS original (UOM re-normalization, UPC
//  auto-approval, standardized_base_upc from JSON) was deliberately NOT
//  migrated — bjs is not served by this GCP job.
//
//    clients enabled for catalog sync : Firestore  (was DynamoDB)
//    matches / tpvr / catalog         : Cloud SQL  (was AWS Aurora)
//    updated-matches report           : GCS bucket (was S3)
//
//  The transform logic (upcCheckSum, resolveBaseUpcFromList) is copied verbatim
//  from the original; only the data-access layer and the
//  node-cron -> Cloud Scheduler trigger changed.
// ----------------------------------------------------------------------------
import moment from 'moment';

import * as aurora from '../stdLib/aurora_resources.js';
import * as firestore from '../stdLib/firestore_resources.js';
import { generateMatchesUpdatedReportWithCatalog, CATALOG_SYNC_STAGE_DIR, removeDirectory } from '../stdLib/node_utils.js';
import {
    notifySlackProcessStart,
    notifySlackProcessCompleted,
    notifySlackStatus
} from '../stdLib/sendSlackNotification.js';

const jobName = 'Catalog Sync Up';

// ------
//  Main
// ------
export async function run() {
    try {
        await aurora.init();

        const mapping = await firestore.getCatalogAttributeMapping();
        if (!mapping) {
            const message = 'catalog_attribute_mapping doc missing in Firestore (catalog_sync_config/attribute_mapping) - '
                + 'run scripts/provision_catalog_attribute_mapping.js. Aborting: no tenant processed.';
            console.log(`${moment().format()} ${jobName} | ${message}`);
            await notifySlackStatus(jobName, 'ERROR', 'all', message, true);
            return;
        }

        const clients = await firestore.getClientsEnabledForCatalogSyncProcess();
        console.log(`${moment().format()} ${jobName} | ${clients.length} client(s) with catalog_syncup_process enabled (ENV=${process.env.ENV || 'dev'})`);

        const forceSync = process.env.CATALOG_SYNC_FORCE === 'true';
        for (const client of clients) {
            await processSyncUp(client, mapping, forceSync);
    }
    } finally {
        // Post-job cleanup: drop the staging dir so the last tenant's report
        // files don't linger in Cloud Run's RAM-backed temp dir after exit.
        removeDirectory(CATALOG_SYNC_STAGE_DIR);
    }
}

// Reads the per-tenant behaviour flags off the Firestore company-code doc.
// These replace the original's hardcoded company_code branches. Only the flags
// the JOB LOOP needs live here — column-level behaviour flags
// (total_size_sync_enabled, json_category_override_enabled, ...) are consumed
// by buildCatalogSyncPlan via the mapping doc's `enabled_by_flag` references,
// which read the same company-code doc directly.
function deriveTenantFlags(client) {
    return {
        // ctc: resolve a single base UPC from a multi-UPC catalog value
        multiUpcResolutionEnabled: !!client.multi_upc_resolution_enabled,
        // staterbros: only sync from catalog rows where is_active
        catalogActiveOnly: !!client.catalog_active_only_sync
    };
}

async function processSyncUp(client, mapping, forceSync = false) {
    const company_code = client.company_code;
    try {
        const suffix = `_${company_code}_${company_code}_${company_code}`;
        const matchesBasePartition = `matches${suffix}`;
        const catalogBasePartition = `catalog${suffix}`;
        const tpvrTable = `tpvr_${company_code}`;
        const flags = deriveTenantFlags(client);

        await notifySlackProcessStart(jobName, company_code);

        const maxCatalogDate = await aurora.getLatestCatalogCaptureDate(catalogBasePartition, company_code);

        const catalogVersionDate = await aurora.getCatalogVersionDate(company_code);
        const columnsInSync = (catalogVersionDate === maxCatalogDate && !forceSync);


        if (columnsInSync) {
            await notifySlackProcessCompleted(jobName, company_code,
                `Catalog Sync Up Completed - Matches already in sync for ${company_code}.`);
            return;
        }

        if (!maxCatalogDate) {
            await notifySlackProcessCompleted(jobName, company_code, `Catalog Sync Up Completed - No catalog present for this company code ${company_code}`);
            return;
        }

        await notifySlackProcessCompleted(jobName, company_code, `Catalog Sync Up Eligible for this company code ${company_code}`);

        const inserted = await aurora.insertToCatalogVersion(company_code, maxCatalogDate);
        if (!inserted) {
            await notifySlackProcessCompleted(jobName, company_code,
                `Catalog Sync Up Failed for this company code ${company_code}, Something went wrong while inserting into catalog_version table`);
            return;
        }

        const catalogActiveChanges = [];
        const changes = await aurora.syncCatalogActiveFlag(
            tpvrTable, catalogBasePartition, maxCatalogDate, company_code
        );
        catalogActiveChanges.push(...changes.map(r => ({
            base_sku: r.base_sku,
            is_catalog_active: r.is_catalog_active,
            action: r.is_catalog_active ? 'reactivated' : 'deactivated'
        })));

        const matchesTouched = [];
        const tpvrTouched = [];

        // 1) matches: sync the tenant's matches base table. The column plan comes
        //    from the Firestore attribute mapping, filtered to the columns that
        //    exist on this tenant's catalog and matches tables.
        //
        //    On AWS Aurora this table had per-competitor child partitions
        //    (discovered via pg_inherits) and the original job looped over them.
        //    On the migrated Cloud SQL instance the matches tables are plain —
        //    every competitor's rows live in matches_{cc}_{cc}_{cc}, distinguished
        //    by the comp_source_store column (the same way match_library_gcs_export
        //    and matchlibrary-baas address them) — so a single pass over the base
        //    table covers all competitors.
        const matchesPlan = await aurora.buildCatalogSyncPlan(catalogBasePartition, matchesBasePartition, mapping, client);
        if (matchesPlan.length) {
            await syncCatalogColumns({
                table: matchesBasePartition,
                catalogPartition: catalogBasePartition,
                maxCatalogDate,
                plan: matchesPlan,
                companyCode: company_code,
                flags
            }, matchesTouched);
        }

        // 2) tpvr: a single per-tenant table (same row shape as matches).
        const tpvrPlan = await aurora.buildCatalogSyncPlan(catalogBasePartition, tpvrTable, mapping, client);
        if (tpvrPlan.length) {
            await syncCatalogColumns({
                table: tpvrTable,
                catalogPartition: catalogBasePartition,
                maxCatalogDate,
                plan: tpvrPlan,
                companyCode: company_code,
                flags
            }, tpvrTouched);
        }

        // Generate separate reports for matches and tpvr
        if (matchesTouched.length) {
            const matchesLoc = await generateMatchesUpdatedReportWithCatalog(company_code, matchesTouched, [], 'matches');
            console.log(`${moment().format()} ${jobName} | Generated matches report at gs://${process.env.gcs_bucket}/${matchesLoc}`);
        }

        if (tpvrTouched.length || catalogActiveChanges.length) {
            const tpvrLoc = await generateMatchesUpdatedReportWithCatalog(company_code, tpvrTouched, catalogActiveChanges, 'tpvr');
            console.log(`${moment().format()} ${jobName} | Generated tpvr report at gs://${process.env.gcs_bucket}/${tpvrLoc}`);
        }

        const totalChanges = matchesTouched.length + tpvrTouched.length + catalogActiveChanges.length;
        if (totalChanges) {
            const msg = `Catalog Sync Up Completed for ${company_code}\n`
                + `- ${matchesTouched.length} attribute update(s) in matches table\n`
                + `- ${tpvrTouched.length} attribute update(s) in tpvr table\n`
                + `- ${catalogActiveChanges.length} is_catalog_active change(s) in tpvr table\n`;
            await notifySlackProcessCompleted(jobName, company_code, msg);
        } else {
            await notifySlackProcessCompleted(jobName, company_code,
                `Catalog Sync Up Completed - No changes found for ${company_code}`);
        }
    } catch (error) {
        console.log(`${moment().format()} - processSyncUp error: ${error}`);
        const message = `There was an error while processSyncUp.\nError: \n${error}`;
        await notifySlackStatus(jobName, 'ERROR', company_code, message, true);
    }
}

// Runs the per-column catalog sync for a single target table (matches_<cc>_<cc>_<cc>
// or tpvr_<cc> — both plain, single tables on the migrated Cloud SQL instance).
// Mirrors the original processSyncUp inner loop, iterating the mapping-derived
// plan (buildCatalogSyncPlan specs) instead of raw catalog column names.
//   table       - the matches/tpvr table to fetch/update against
//   accumulator - collects touched rows for the report
async function syncCatalogColumns({ table, catalogPartition, maxCatalogDate, plan, companyCode, flags }, accumulator) {
    for (const spec of plan) {
        const matches_response = await aurora.fetchMatchDifferentWithCatalog(
            table, catalogPartition, maxCatalogDate, spec, companyCode,
            { catalogActiveOnly: flags.catalogActiveOnly }
        );
        if (!matches_response || !matches_response.length){ 
            continue;
        }

        const compactRecord = (row) => ({
            match_id: row.match_id,
            base_sku: row.base_sku,
            column: spec.target,
            old_value: row[spec.target] || null,
            new_value: row.updated_value,
            comp_source_store: row.comp_source_store
        });

        if (flags.multiUpcResolutionEnabled && spec.target === 'base_upc') {
            const current_time = moment().format('YYYY-MM-DD HH:mm:ss');
            const attributesUpdated = matches_response.reduce((acc, match) => {
                const upcList = (match.updated_value || '').trim().split(/[\s,]+/).filter(Boolean);
                // Catalog upc cleared (null/empty/whitespace): propagate the clear.
                // The fetch's null-safe diff already excludes rows where both sides
                // are null/'' — the JS guard covers what SQL can't: a whitespace-only
                // catalog value counts as empty here but not in the SQL diff.
                if (!upcList.length) {
                    if (!match.base_upc || !match.base_upc.trim()) return acc;
                    match.updated_value = match.updated_value;
                    accumulator.push(compactRecord(match));
                    acc[match.match_id] = { base_upc: null, internal_notes: `match_update_by_catalog_processor: ${current_time}` };
                    return acc;
                }
                const resolved = resolveBaseUpcFromList(upcList, match.comp_upc, match.model_used);
                if (match.base_upc === resolved) return acc;
                match.updated_value = resolved;
                accumulator.push(compactRecord(match));
                acc[match.match_id] = { base_upc: resolved, internal_notes: `match_update_by_catalog_processor: ${current_time}` };
                return acc;
            }, {});
            if (Object.keys(attributesUpdated).length > 0) {
                await aurora.updateAttributes(attributesUpdated, companyCode, table);
            }
        } else {
            for (const match of matches_response) {
                accumulator.push(compactRecord(match));
            }
            await aurora.updateMatchWithCatalog(
                table, catalogPartition, maxCatalogDate, spec, companyCode,
                { catalogActiveOnly: flags.catalogActiveOnly }
            );
        }
    }
    console.log(`${moment().format()} ${jobName} | ${table} catalog sync completed for ${companyCode} - ${plan.length} column(s) processed`);
}

// ----------------------------------------------------------------------------
//  Transform helpers — copied verbatim from the original catalog_sync_processor
//  (only require -> import adjusted). Pure business logic.
//  NOTE: the bjs-specific helpers (processNormalizedAtrributes, processUpcFields)
//  were deliberately NOT migrated — bjs is not served by this GCP job. The
//  auto-approval helpers (checkforSpecialConditions, buildUpcAutoApprovalUpdate)
//  were removed from this job — catalog sync no longer auto-approves matches.
// ----------------------------------------------------------------------------
function upcCheckSum(baseUpc, compUpc) {
    if (!baseUpc || !compUpc) {
        return false
    }
    let base_upc = Number(baseUpc).toString();
    let comp_upc = Number(compUpc).toString();
    let len1 = base_upc.length;
    let len2 = comp_upc.length;

    // if base_upc is not a number or comp_upc is not a number
    if (isNaN(base_upc) || isNaN(comp_upc)) {
        return false;
    }

    if (len1 < 3 || len2 < 3 || base_upc.substring(0, len1 - 1) === comp_upc.substring(0, len2 - 1)) {
        return true
    }

    // checksum digit at competitor upc
    if (len1 + 1 == len2) {
        return upcCheckSum(base_upc, comp_upc.substring(0, len2 - 1))
    }
    // checksum digit at client upc
    if (len1 == len2 + 1) {
        return upcCheckSum(base_upc.substring(0, len1 - 1), comp_upc)
    }

    if (len1 === len2 && base_upc !== comp_upc) {
        return false;
    }

    return false
}

// Resolve a single base UPC from an already-split catalog UPC list (the caller
// parses and handles the empty case). For upc-based models, prefer the entry
// that checksum-matches the competitor UPC; otherwise take the first.
// Adapted from PmtScheduleProcessing scripts/upc_matches_auto_approval.js.
function resolveBaseUpcFromList(upcList, compUpc, modelUsed) {
    if (upcList.length === 1) return upcList[0];
    const isUpcModel = modelUsed && modelUsed.toLowerCase().includes('upc');
    if (isUpcModel && compUpc) {
        const matched = upcList.find(upc => upcCheckSum(upc, compUpc));
        return matched || upcList[0];
    }
    return upcList[0];
}
