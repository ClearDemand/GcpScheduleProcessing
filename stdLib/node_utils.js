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

// Catalog-sync report. Ported from PmtScheduleProcessing node_utils
// .generateMatchesUpdatedReportWithCatalog: writes a CSV of the updated matches
// plus a summary .txt and uploads both to the output bucket. The original wrote
// to a local `S3Uploads/` dir and pushed to the `bungee.productmatching` S3
// bucket; here we stage under the OS temp dir and upload to GCS (gcs_bucket),
// keeping the identical `catalog_sync_updated_matches/...` key layout. Returns
// the upload key prefix on success, or false when there was nothing to upload.
export async function generateMatchesUpdatedReportWithCatalog(company_code, metrics) {
    try {
        const stageDir = CATALOG_SYNC_STAGE_DIR;
        if (!fs.existsSync(stageDir)) fs.mkdirSync(stageDir, { recursive: true });

        let dataUploaded = false;
        const localRun = process.env.ENV != 'prod';
        const date = moment();
        const keyPrefix = `catalog_sync_updated_matches/company_code=${company_code}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/`;

        const deletedFilename = `matches_cleanup_${date.format('YYYY-MM-DD-HH')}${localRun ? '_local' : ''}.csv`;
        const deleted = metrics;

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
            const baseSkusWithUrlMismatch = metrics.filter(item => item.update_reason === 'base_url is not matching').map(item => item.base_sku);
            const baseSkusWithSizeMismatch = metrics.filter(item => item.update_reason === 'base_size is not matching').map(item => item.base_sku);
            const baseSkusWithUomMismatch = metrics.filter(item => item.update_reason === 'base_uom is not matching').map(item => item.base_sku);

            const mailContent = `
            Catalog Sync Up Report for ${company_code}:


            - Total base_sku's mismatch: ${metrics.length}
            - Unique base_sku's mismatch: ${uniqueBaseSkus.length}
            - base_sku's with 'base_url not matching': ${baseSkusWithUrlMismatch.length}
            - base_sku's with 'base_size not matching': ${baseSkusWithSizeMismatch.length}
            - base_sku's with 'base_uom not matching': ${baseSkusWithUomMismatch.length}
            `;

            const reportFilename = `report_${company_code}_${date.format('YYYY-MM-DD-HH')}${localRun ? '_local' : ''}.txt`;
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
