// ----------------------------------------------------------------------------
//  Job: Auto Ingestion Processor (GCP)
//
//  Trigger-only (no Cloud Scheduler entry) — invoked on demand via the Cloud
//  Run Admin API by matchlibrary-baas's POST /matches/auto-ingestion/trigger,
//  after it inserts a row (one per client) into auto_ingestion_trigger_queue.
//
//  Ported from PmtScheduleProcessing/scripts/upc_matches_auto_approval.js's
//  upcMatchesAutoApproval()/processApproval() — but the actual per-suggestion
//  ingest decision now lives in matchlibrary-baas's ingestMatch()
//  (source: 'auto_ingestion'), reached via HTTP, not reimplemented here.
//  This job's job is: claim → fetch Athena suggestions → dedup/group →
//  call matchlibrary-baas in chunks → report.
//
//  Two deliberate deviations from the reference script, not oversights:
//   1. The reference script loops `for (base_source_store of
//      companyKey.base_banners)` and re-runs the (identical, banner-unfiltered)
//      Athena query inside that loop -- getUpcMatchesToProcess's WHERE clause
//      has no base_source_store filter at all, so every banner re-queries and
//      re-processes the exact same suggestion set. The loop's only other
//      purpose in the reference script was fetching a per-banner
//      latestCatalogCaptureDate to thread into processApproval() -- which this
//      architecture doesn't need (ingestMatch()'s catalog lookup takes no date
//      param). So there's no banner loop here: Athena is queried once per
//      client, and each returned row already carries its own base_source_store.
//   2. Given (1) removes the loop entirely, the reference script's `return`
//      (not `continue`) on an empty result set is moot here -- there's only
//      ever one fetch per client now, so no "first banner empty, skip the
//      rest" failure mode can occur.
//
//    company-code library / base_banners / mlDrivenUpcApproval : Firestore (was DynamoDB)
//    ML-suggested matches                                      : Athena, cross-cloud (AWS)
//    per-suggestion ingest decision                             : HTTP -> matchlibrary-baas
//    trigger queue claim/status                                 : Cloud SQL (was Aurora)
//    output report                                              : S3 bucket, cross-cloud (AWS)
//                                                                  -- same bucket/pattern as
//                                                                  PmtScheduleProcessing's
//                                                                  upc_matches_auto_approval.js,
//                                                                  kept as-is for now instead of
//                                                                  migrating to GCS.
// ----------------------------------------------------------------------------
import fs from 'fs';
import os from 'os';
import path from 'path';
import moment from 'moment';
import axios from 'axios';

import * as aurora from '../stdLib/aurora_resources.js';
import * as firestore from '../stdLib/firestore_resources.js';
import * as athena from '../stdLib/athena_resources.js';
import * as s3 from '../stdLib/s3_resources.js';
import * as autoIngestion from '../stdLib/auto_ingestion_resources.js';
import { createCsvFile, initExportFolder, removeDirectory } from '../stdLib/node_utils.js';
import { notifySlack, notifySlackProcessStart, notifySlackProcessCompleted, notifySlackStatus } from '../stdLib/sendSlackNotification.js';

const jobName = 'Auto Ingestion Processor';

const EXPORT_DIR = path.join(os.tmpdir(), 'AutoIngestionProcessor');
const stagePath = name => path.join(EXPORT_DIR, name);

const MATCH_TYPES = ['exact', 'equivalent', 'undetermined', 'similar'];
const CHUNK_SIZE = 50;
const CHUNK_SLEEP_MS = 5000;
// "Customer Fastlane" model_used rows are a different ingestion flavor
// entirely (see PmtScheduleProcessing/scripts/customer_fastlane_duplicates.js)
// -- out of scope for this pipeline (see migration plan's "Out of scope for
// V1"). getUpcMatchesToProcess's query doesn't filter them out, so skip them
// here rather than route them through ingestMatch(source:'auto_ingestion').
const SKIPPED_MODEL_USED = 'Customer Fastlane';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function matchlibraryBaasUrl() {
    // Same derivation as manualMatchProcessor/poll.js's API_URL.
    return process.env.ENV === 'local'
        ? 'http://localhost:8080'
        : `https://us-east5-${process.env.GCP_PROJECT_ID}.cloudfunctions.net/matchlibrary-baas`;
}

// Separates one client's day of Athena suggestions into per-match-type groups
// (batched/concurrent processing) and same-key duplicates (processed
// sequentially) -- ported from upcMatchesAutoApproval's dedup loop. A
// suggestion's dedup key is (base_sku, comp_source_store, match): the first
// occurrence of a key goes into its match-type group, later occurrences of
// the same key go into `duplicates`.
function groupSuggestions(suggestions) {
    const seen = new Set();
    const groups = { exact: [], equivalent: [], undetermined: [], similar: [] };
    const duplicates = [];
    let skippedCustomerFastlane = 0;

    for (const matchType of MATCH_TYPES) {
        for (const suggestion of suggestions) {
            if (suggestion.match !== matchType) continue;
            if (suggestion.model_used === SKIPPED_MODEL_USED) {
                skippedCustomerFastlane++;
                continue;
            }
            const key = `${suggestion.base_sku?.toLowerCase()}_${suggestion.comp_source_store}_${suggestion.match}`;
            if (seen.has(key)) {
                duplicates.push(suggestion);
            } else {
                seen.add(key);
                groups[matchType].push(suggestion);
            }
        }
    }
    return { groups, duplicates, skippedCustomerFastlane };
}

// POSTs a batch of Athena suggestion rows to matchlibrary-baas's
// POST /auto-ingestion/process-match, in chunks (default: CHUNK_SIZE,
// concurrent within matchlibrary-baas; sequential across chunks with a pause
// between them, mirroring the reference script's own 50-per-batch/5s-sleep
// pacing). Passing chunkSize=1 processes items strictly one at a time, used
// for same-key duplicates so they don't race matchIngestion's own dedup logic
// against each other.
async function ingestInChunks(companyCode, processId, suggestions, chunkSize = CHUNK_SIZE) {
    const results = [];
    for (let i = 0; i < suggestions.length; i += chunkSize) {
        const chunk = suggestions.slice(i, i + chunkSize);
        try {
            const { data } = await axios.post(`${matchlibraryBaasUrl()}/auto-ingestion/process-match`, {
                companyCode,
                processId,
                matches: chunk,
            });
            results.push(...(data?.results || []));
        } catch (err) {
            console.error(`ingestInChunks error for ${companyCode}, chunk starting at ${i}: ${err.message}`);
            results.push(...chunk.map(s => ({
                base_sku: s.base_sku,
                comp_sku: s.comp_sku,
                comp_source_store: s.comp_source_store,
                success: false,
                reason: 'request_failed',
                message: err.message,
            })));
        }
        if (i + chunkSize < suggestions.length) await sleep(CHUNK_SLEEP_MS);
    }
    return results;
}

async function uploadReport(companyCode, results) {
    try {
        if (!results || results.length === 0) return null;
        const date = moment();
        const filename = `${companyCode}-${date.format('YYYY-MM-DD-HH-mm')}.csv`;
        const created = await createCsvFile(stagePath(filename), results);
        if (!created) return null;

        const key = `auto_ingestion/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/${filename}`;
        await s3.uploadObject({ Key: key, Body: fs.readFileSync(stagePath(filename)) });
        return await s3.getSignedDownloadUrl(key);
    } catch (err) {
        console.error(`uploadReport error for ${companyCode}: ${err.message}`);
        return null;
    }
}

// ------
//  Main
// ------
export async function run() {
    await aurora.init();
    await athena.init();
    await s3.init();

    const claimed = await autoIngestion.claimNextPendingClient();
    if (!claimed) {
        console.log(`${moment().format()} ${jobName} | no pending auto_ingestion_trigger_queue rows to claim — no-op exit`);
        return;
    }

    const { id, process_id: processId, company_code: companyCode } = claimed;

    try {
        const companyKey = await firestore.getCompanyKey(companyCode);
        if (!companyKey) {
            await autoIngestion.markClientStatus(id, 'failed', `Unknown company_code ${companyCode}`);
            await notifySlackStatus(jobName, 'ERROR', companyCode, `Unknown company_code ${companyCode}`, true);
            return;
        }

        // Defensive re-check, mirrors sqsPoll.js's own re-validation via
        // getClientsForMlDrivenUpcApproval() before acting on a queued
        // request -- the eligibility flag may have changed between when
        // scheduler_trigger_api.js validated it and when this execution runs.
        if (companyKey.mlDrivenUpcApproval !== true) {
            await autoIngestion.markClientStatus(id, 'skipped', 'mlDrivenUpcApproval not enabled');
            return;
        }

        await notifySlackProcessStart(jobName, companyCode);
        initExportFolder(EXPORT_DIR);

        const loadDate = moment().format('YYYY-MM-DD');
        const suggestions = await athena.getUpcMatchesToProcess(companyCode, loadDate);

        if (!suggestions || suggestions.length === 0) {
            await autoIngestion.markClientStatus(id, 'completed', 'No Athena suggestions found');
            await notifySlackProcessCompleted(jobName, companyCode, 'No Data Found In the View');
            return;
        }

        const { groups, duplicates, skippedCustomerFastlane } = groupSuggestions(suggestions);
        if (skippedCustomerFastlane > 0) {
            console.log(`${moment().format()} ${jobName} | skipped ${skippedCustomerFastlane} 'Customer Fastlane' model_used rows for ${companyCode} — out of scope for this pipeline`);
        }

        const allResults = [];
        for (const matchType of MATCH_TYPES) {
            const group = groups[matchType];
            if (!group || group.length === 0) continue;
            await notifySlack(`*Auto Ingestion*\nClient: *${companyCode}*\nType: *${matchType}*\nSuggestions: *${group.length}*`);
            allResults.push(...await ingestInChunks(companyCode, processId, group));
        }
        if (duplicates.length > 0) {
            await notifySlack(`*Auto Ingestion*\nClient: *${companyCode}*\nType: *Duplicate Matches*\nSuggestions: *${duplicates.length}*`);
            allResults.push(...await ingestInChunks(companyCode, processId, duplicates, 1));
        }

        const { success, failed } = allResults.reduce((acc, r) => {
            if (r.success) acc.success++; else acc.failed++;
            return acc;
        }, { success: 0, failed: 0 });

        const reportLink = await uploadReport(companyCode, allResults);

        await autoIngestion.markClientStatus(id, 'completed', `success=${success} failed=${failed}`);

        const summary = `*Success:* ${success}\n*Failed:* ${failed}`
            + (reportLink ? `\n<${reportLink}|Download Report>` : '');
        await notifySlackProcessCompleted(jobName, companyCode, summary);
    } catch (err) {
        console.error(`${moment().format()} ${jobName} failed for ${companyCode}: ${err.stack || err}`);
        await autoIngestion.markClientStatus(id, 'failed', err.message);
        await notifySlackStatus(jobName, 'ERROR', companyCode, `Immediate Action Required: ${err.message}`, true);
        // The queue row is already flipped to 'failed' above, so a Cloud Run
        // auto-retry of this whole execution is safe -- it'll simply claim a
        // different pending row (or no-op if none remain).
        throw err;
    } finally {
        removeDirectory(EXPORT_DIR);
    }
}
