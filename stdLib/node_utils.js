// ----------------------------------------------------------------------------
//  Local file utilities (ES module)
//  Ported from PmtScheduleProcessing/stdLib/node_utils.js — CSV writing and
//  staging-directory management. Paths are absolute (under the OS temp dir)
//  so this works on Cloud Run's writable temp dir.
// ----------------------------------------------------------------------------
import fs from 'fs';
import os from 'os';
import path from 'path';
import moment from 'moment';
import csvWriterPkg from 'csv-writer';
import * as gcs from './gcs_resources.js';
import { isSentinelValue } from './match_update_constants.js';

const { createObjectCsvWriter } = csvWriterPkg;

// Staging dir for the catalog-sync report files (RAM-backed temp dir on Cloud
// Run). Exported so the job can tear it down as post-job cleanup.
export const CATALOG_SYNC_STAGE_DIR = path.join(os.tmpdir(), 'CatalogSyncReport');

// Writes an array of objects to a CSV at an absolute path.
export async function createCsvFile(fullPath, data) {
    try {
        if (!data || !Array.isArray(data) || data.length === 0) {
            return false;
        }

        const headers = Object.keys(data[0]).map(v => ({ id: v, title: v }));
        const writer = createObjectCsvWriter({ path: fullPath, header: headers });
        await writer.writeRecords(data);
        return true;
    } catch (error) {
        console.log(`${moment().format()} - error in createCsvFile: ${error.message}`);
        return false;
    }
}

// Deletes all files (non-recursive) in a directory.
export function deleteFilesInDirectory(directory) {
    try {
        if (!fs.existsSync(directory)) return;
        for (const file of fs.readdirSync(directory)) {
            const p = path.join(directory, file);
            if (fs.statSync(p).isFile()) fs.unlinkSync(p);
        }
    } catch (error) {
        console.log(`deleteFilesInDirectory error: ${error.message}`);
    }
}

// Ensures the export staging directory exists and is empty.
export function initExportFolder(exportDir) {
    if (!fs.existsSync(exportDir)) {
        fs.mkdirSync(exportDir, { recursive: true });
    }
    deleteFilesInDirectory(exportDir);
}

// Removes a staging directory and everything under it. Use as post-job cleanup
// so the last tenant's staged files don't linger in Cloud Run's RAM-backed
// temp dir after the job exits. Best-effort: never throws.
export function removeDirectory(directory) {
    try {
        fs.rmSync(directory, { recursive: true, force: true });
    } catch (error) {
        console.log(`removeDirectory error: ${error.message}`);
    }
}

// Match-update-processor report CSV. Ported from PmtScheduleProcessing
// node_utils.createCsvForMatchUpdateProcessor — writes to an absolute path
// (staged under os.tmpdir() by the caller) instead of the old relative
// `S3Uploads/` dir. `data` is the batch's expanded per-match result array
// (each item has `.processed.modified`/`.processed.status` plus flattened
// `updated_<key>`/`<key>` fields, per jobs/match_update_processor.js).
export async function createMatchUpdateReport(fullPath, data, keysToUpdate) {
    try {
        if (!data || !Array.isArray(data) || data.length === 0) {
            return false;
        }

        const keysToProcess = [...keysToUpdate];
        if (keysToUpdate.includes('comp_brand')) {
            keysToProcess.push('comp_brandtype');
        }

        for (const match of data) {
            if (!match.processed?.modified) continue;
            for (const key of keysToProcess) {
                if (match[`updated_${key}`]) continue;
                let value = match[key];
                if (typeof value === 'object' && value !== null) {
                    value = JSON.stringify(value);
                } else if (isSentinelValue(value)) {
                    value = null;
                }
                match[`updated_${key}`] = value || null;
            }
        }

        let headers = ['match_id', 'company_code', 'base_sku', 'base_source_store', 'comp_sku', 'comp_source_store', 'modified', 'status'];
        for (const key of keysToProcess) {
            headers.push(`updated_${key}`, key);
        }
        headers = headers.map(v => ({ id: v, title: v }));

        const writer = createObjectCsvWriter({ path: fullPath, header: headers });
        await writer.writeRecords(data);
        return true;
    } catch (error) {
        console.log(`${moment().format()} - error in createMatchUpdateReport: ${error.message}`);
        return false;
    }
}

// Catalog-sync report. Ported from PmtScheduleProcessing node_utils
// .generateMatchesUpdatedReportWithCatalog: writes a CSV of the updated matches
// plus a summary .txt and uploads both to the output bucket. The original wrote
// to a local `S3Uploads/` dir and pushed to the `bungee.productmatching` S3
// bucket; here we stage under the OS temp dir and upload to GCS (gcs_bucket),
// keeping the identical `catalog_sync_updated_matches/...` key layout. Returns
// the upload key prefix on success, or false when there was nothing to upload.
//
// `catalogActiveChanges` (optional) is the list of tpvr rows whose
// is_catalog_active flag flipped this run (from syncCatalogActiveFlag). When
// present it is written as a second CSV alongside the updated-matches CSV and
// its counts are added to the summary.
export async function generateMatchesUpdatedReportWithCatalog(company_code, metrics, catalogActiveChanges = [], reportType = 'matches') {
    try {
        metrics = metrics || [];
        catalogActiveChanges = catalogActiveChanges || [];

        const stageDir = CATALOG_SYNC_STAGE_DIR;
        if (!fs.existsSync(stageDir)) fs.mkdirSync(stageDir, { recursive: true });

        let dataUploaded = false;
        const localRun = process.env.ENV != 'prod';
        const date = moment();
        const keyPrefix = `catalog_sync_updated_matches/company_code=${company_code}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/`;

        const deletedFilename = `${reportType}_updated_${date.format('YYYY-MM-DD-HH')}${localRun ? '_local' : ''}.csv`;
        const deleted = metrics;

        // Second CSV: tpvr rows whose is_catalog_active flag changed this run.
        if (catalogActiveChanges.length > 0) {
            const caColumns = Object.keys(catalogActiveChanges[0]);
            const caHeader = caColumns.map(col => ({ id: col, title: col }));
            const caFilename = `catalog_active_changes_${date.format('YYYY-MM-DD-HH')}${localRun ? '_local' : ''}.csv`;
            const caPath = path.join(stageDir, caFilename);
            const caCsvWriter = createObjectCsvWriter({ path: caPath, header: caHeader });

            await caCsvWriter.writeRecords(catalogActiveChanges);
            await gcs.uploadObject({
                Key: `${keyPrefix}${caFilename}`,
                Body: fs.readFileSync(caPath)
            });
            dataUploaded = true;
        }

        if (deleted.length > 0) {
            const columns = Object.keys(deleted[0]);
            const header = columns.map(col => ({ id: col, title: col }));
            const deletedPath = path.join(stageDir, deletedFilename);
            const deletedCsvWriter = createObjectCsvWriter({ path: deletedPath, header });

            await deletedCsvWriter.writeRecords(deleted);
            await gcs.uploadObject({
                Key: `${keyPrefix}${deletedFilename}`,
                Body: fs.readFileSync(deletedPath)
            });
            dataUploaded = true;
        }

        if (dataUploaded) {
            const uniqueBaseSkus = [...new Set(metrics.map(item => item.base_sku))];
            const baseSkusWithUrlMismatch = metrics.filter(item => item.column === 'base_url').map(item => item.base_sku);
            const baseSkusWithSizeMismatch = metrics.filter(item => item.column === 'base_size').map(item => item.base_sku);
            const baseSkusWithUomMismatch = metrics.filter(item => item.column === 'base_uom').map(item => item.base_sku);

            const deactivated = catalogActiveChanges.filter(r => r.is_catalog_active === false).length;
            const reactivated = catalogActiveChanges.filter(r => r.is_catalog_active === true).length;

            const mailContent = `
            Catalog Sync Up Report for ${company_code}:


            - Total base_sku's mismatch: ${metrics.length}
            - Unique base_sku's mismatch: ${uniqueBaseSkus.length}
            - base_sku's with 'base_url not matching': ${baseSkusWithUrlMismatch.length}
            - base_sku's with 'base_size not matching': ${baseSkusWithSizeMismatch.length}
            - base_sku's with 'base_uom not matching': ${baseSkusWithUomMismatch.length}
            - tpvr is_catalog_active set false (sku dropped from catalog): ${deactivated}
            - tpvr is_catalog_active set true (sku back in catalog): ${reactivated}
            `;

            const reportFilename = `report_${reportType}_${company_code}_${date.format('YYYY-MM-DD-HH')}${localRun ? '_local' : ''}.txt`;
            const reportPath = path.join(stageDir, reportFilename);
            fs.writeFileSync(reportPath, mailContent);

            await gcs.uploadObject({
                Key: `${keyPrefix}${reportFilename}`,
                Body: fs.readFileSync(reportPath)
            });

            return keyPrefix;
        }
        return false;
    } catch (error) {
        console.log(`${moment().format()} - error in generateMatchesUpdatedReportWithCatalog: ${error.message}`);
        return false;
    }
}
