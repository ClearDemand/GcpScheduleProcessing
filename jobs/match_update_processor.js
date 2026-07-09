// ----------------------------------------------------------------------------
//  Job: Match Update Processor (GCP)
//
//  Trigger-only (no Cloud Scheduler entry) — invoked on demand via the Cloud
//  Run Admin API by whatever service inserts a row into matches_update_queue.
//  Ported from PmtScheduleProcessing/poll.js, which ran this as an always-on
//  setInterval loop; here it's a single claim-and-process-then-exit run.
//
//    company-code library / competitor mapping : Firestore  (was DynamoDB)
//    matches / update queue / audit trail       : Cloud SQL (was Aurora)
//    Global Product Catalog (process_type=gpc)  : DynamoDB, cross-cloud (AWS)
//    pricing dataset (process_type=pricing_parquet) : Athena/S3, cross-cloud (AWS)
//    output reports                             : GCS bucket (was S3)
//    report delivery email                      : SES SMTP, cross-cloud (AWS)
// ----------------------------------------------------------------------------
import fs from 'fs';
import os from 'os';
import path from 'path';
import moment from 'moment';

import * as aurora from '../stdLib/aurora_resources.js';
import * as firestore from '../stdLib/firestore_resources.js';
import * as matchUpdate from '../stdLib/match_update_resources.js';
import * as dynamo from '../stdLib/dynamo_resources.js';
import * as athena from '../stdLib/athena_resources.js';
import * as ses from '../stdLib/ses_resources.js';
import * as gcs from '../stdLib/gcs_resources.js';
import { createMatchUpdateReport, initExportFolder, removeDirectory } from '../stdLib/node_utils.js';
import { notifySlack, notifySlackStatus } from '../stdLib/sendSlackNotification.js';
import { PRICING_PARQUET_KEY_MAP, PRICING_PARQUET_CUSTOM_ATTRIBUTE_KEYS } from '../stdLib/match_update_constants.js';

const jobName = 'Match Update Processor';

// Local staging dir for the per-batch report CSV before upload, same pattern
// as match_library_gcs_export.js (Cloud Run's only writable path).
const EXPORT_DIR = path.join(os.tmpdir(), 'MatchUpdateProcessor');
const stagePath = name => path.join(EXPORT_DIR, name);

const BATCH_SIZE = 500;
const SUB_CHUNK_SIZE = 50;

// ------
//  Main
// ------
export async function run() {
    await aurora.init();

    const claimed = await matchUpdate.claimNextPendingGroup();
    if (!claimed || claimed.length === 0) {
        // Expected steady state for a trigger-only job: a duplicate/stale
        // trigger, or this execution lost a race to another concurrent one.
        console.log(`${moment().format()} ${jobName} | no pending matches_update_queue rows to claim — no-op exit`);
        return;
    }

    const first = claimed[0];
    const processId = first.process_id;
    const companyCode = first.company_code;
    const processType = first.process_type;
    const updateLevel = first.update_level;
    const userEmail = first.ingestion_user;
    const baseSourceStore = first.base_source_store;
    const compSourceStore = first.comp_source_store;

    try {
        const companyKey = await firestore.getCompanyKey(companyCode);
        if (!companyKey) {
            await matchUpdate.markGroupStatus(companyCode, processId, 'failed', 'Unknown company_code');
            await notifySlackStatus(jobName, 'ERROR', processId, `Unknown company_code ${companyCode}`, true);
            return;
        }

        const additional = first.additional_attributes || {};
        const keysToUpdate = additional.keys;
        if (!keysToUpdate || keysToUpdate.length === 0) {
            await matchUpdate.markGroupStatus(companyCode, processId, 'failed', 'No Keys Received');
            await notifySlack(`Keys To Update is Empty, For Process Id - *${processId}*`);
            return;
        }

        let pendingListings = claimed;
        if (updateLevel === 'competitor') {
            pendingListings = await matchUpdate.getAllCompetitorMatches(
                companyCode, baseSourceStore, compSourceStore,
                additional.active === true, additional.inactive_only === true,
                additional.start_date, additional.end_date
            );
        }

        if (pendingListings.length === 0) {
            await matchUpdate.markGroupStatus(companyCode, processId, 'completed', 'No Matches retrieved for processing');
            await notifySlack(`No Matches to Process, Process ID - *${processId}*`);
            return;
        }

        await notifySlack(`Request Picked Up for Processing, Process Id - *${processId}*\nTotal Matches for Processing - *${pendingListings.length}*`);

        if (processType === 'gpc') {
            await dynamo.init();
        } else if (processType === 'pricing_parquet') {
            await athena.init();
        }

        initExportFolder(EXPORT_DIR);

        // Group by match_id (mirrors old poll.js) so batching/processing works
        // off match identity, not raw queue-row order.
        const matchesMap = {};
        for (const match of pendingListings) {
            const key = `${match.match_id}`;
            if (!matchesMap[key]) matchesMap[key] = [];
            matchesMap[key].push(match);
        }

        const totalBatches = Math.ceil(pendingListings.length / BATCH_SIZE);
        const downloadLinks = [];
        let batchMatches = [];
        let batchIndex = 1;

        for (let index = 0; index < pendingListings.length; index++) {
            batchMatches.push(matchesMap[`${pendingListings[index].match_id}`] || []);
            if (batchMatches.length % BATCH_SIZE === 0 || index === pendingListings.length - 1) {
                if (processType === 'gpc' || processType === 'pricing_parquet') {
                    const link = await processBatch(companyKey, batchMatches, processId, keysToUpdate, updateLevel, batchIndex, totalBatches, processType);
                    if (link) downloadLinks.push(link);
                }
                await notifySlack(`*Batch Status*\nProcess ID - *${processId}*\nBatch *${batchIndex}* out of *${totalBatches}*`);
                batchIndex++;
                batchMatches = [];
            }
        }

        await notifySlack(`Execution Completed for Process ID - *${processId}*`);

        if (downloadLinks.length > 0) {
            const linksHtml = downloadLinks.map((link, i) => `<a href="${link}">Match Library Data Batch - ${i + 1}</a><br>`).join('');
            await ses.init();
            await ses.sendReportEmail(
                userEmail,
                `Match Update Request Reports For Process - ${processId}`,
                `<p>Please click the below links to download Match Update Reports</p>${linksHtml}<p>Regards,<br>Bungee Team</p>`
            );
        }
    } catch (err) {
        console.error(`${moment().format()} ${jobName} failed for process ${processId}: ${err.stack || err}`);
        await matchUpdate.markGroupStatus(companyCode, processId, 'failed', 'Unexpected Error, Failed to process');
        await notifySlackStatus(jobName, 'ERROR', processId, `Immediate Action Required: ${err.message}`, true);
        // Queue rows are already flipped to 'failed' above, so a Cloud Run
        // auto-retry of this whole execution is safe — it'll simply find
        // nothing pending for this process_id.
        throw err;
    } finally {
        removeDirectory(EXPORT_DIR);
    }
}

// Processes one batch (<=500 matches) of a claimed group: fetches
// authoritative attribute values, syncs each match, writes audit trail rows
// and a CSV report, uploads it, and returns a signed download link (or null
// on failure — logged/Slack-alerted, not thrown, so remaining batches still run).
async function processBatch(companyKey, batchMatches, processId, keysToUpdate, updateLevel, batchIndex, totalBatches, processType) {
    try {
        let pricingRowsMap = null;
        if (processType === 'pricing_parquet') {
            const flatMatches = batchMatches.map(item => item[0]).filter(Boolean);
            const pricingRows = await athena.getPricingParquetRowsForMatches(
                companyKey.company_code, PRICING_PARQUET_KEY_MAP, flatMatches, keysToUpdate, PRICING_PARQUET_CUSTOM_ATTRIBUTE_KEYS
            );
            pricingRowsMap = new Map();
            for (const row of pricingRows || []) {
                const key = `${(row.base_sku || '').toLowerCase()}|${(row.comp_source_store || '').toLowerCase()}|${(row.comp_sku || '').toLowerCase()}`;
                pricingRowsMap.set(key, row);
            }
        }

        let output = [];
        let promises = [];
        for (let index = 0; index < batchMatches.length; index++) {
            const match = batchMatches[index][0];
            promises.push(processType === 'gpc'
                ? matchUpdate.syncRecords(match, keysToUpdate, companyKey)
                : matchUpdate.syncPricingParquetRecords(match, keysToUpdate, companyKey, pricingRowsMap, PRICING_PARQUET_KEY_MAP));

            if (promises.length % SUB_CHUNK_SIZE === 0 || index === batchMatches.length - 1) {
                output = output.concat(await Promise.all(promises));
                promises = [];
            }
        }

        const { success, failed } = output.reduce((acc, item) => {
            if (item.processed?.modified) acc.success++; else acc.failed++;
            return acc;
        }, { success: 0, failed: 0 });

        const expandedData = output.map(obj => ({ ...obj, ...obj.updated, ...obj.processed }));

        if (updateLevel !== 'competitor') {
            const updates = output.map(obj => ({
                id: obj.id,
                status: obj.processed?.modified ? 'completed' : 'failed',
                statusComments: obj.processed?.status
            }));
            await matchUpdate.updatePendingListingStatuses(companyKey.company_code, updates);
        } else if (batchIndex === totalBatches) {
            await matchUpdate.markGroupStatus(companyKey.company_code, processId, 'completed', 'successfully processed the request');
        }

        const auditRows = output
            .filter(m => m.processed?.modified)
            .map(m => ({ ...m.auditData, match_id: m.match_id, base_sku: m.base_sku, comp_sku: m.comp_sku, comp_source_store: m.comp_source_store }));
        if (auditRows.length > 0) {
            const auditLabel = processType === 'pricing_parquet' ? 'PRICING_PARQUET_SYNC' : 'GPC_SYNC';
            await aurora.insertIntoAuditRecords(companyKey.company_code, auditRows, auditLabel, 'update', `match_update_processor - ${processId}`);
        }

        const date = moment();
        const outputFilename = `${processId}-output-${date.unix()}.csv`;
        const fileCreated = await createMatchUpdateReport(stagePath(outputFilename), expandedData, keysToUpdate);
        if (!fileCreated) {
            await notifySlack(`Processing Completed For the Process Id - *${processId}* and Batch Index - *${batchIndex}*\nSuccess - *${success}*\nFailed - *${failed}*\nError in Report Generation`);
            return null;
        }

        const key = `match_update_processor/${processType}/output/company_code=${companyKey.company_code}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/${outputFilename}`;
        await gcs.uploadObject({ Key: key, Body: fs.readFileSync(stagePath(outputFilename)) });
        const downloadLink = await gcs.getSignedDownloadUrl(key);

        await notifySlack(`Processing Completed For the Process Id - *${processId}* and Batch Index - *${batchIndex}*\nSuccess - *${success}*\nFailed - *${failed}*\nDownload Link - <${downloadLink}|Download Report>`);
        return downloadLink;
    } catch (error) {
        console.log(`${moment().format()} processBatch error for process ${processId}, batch ${batchIndex}: ${error.stack || error}`);
        await notifySlackStatus(jobName, 'ERROR', processId, `SYNC Matches Failed, Batch *${batchIndex}*: ${error.message}`, true);
        return null;
    }
}
