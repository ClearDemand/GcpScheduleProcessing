// ----------------------------------------------------------------------------
//  Local file utilities (ES module)
//  Ported from PmtScheduleProcessing/stdLib/node_utils.js — CSV writing and
//  staging-directory management. Paths are absolute (under the OS temp dir)
//  so this works on Cloud Run's writable temp dir.
// ----------------------------------------------------------------------------
import fs from 'fs';
import path from 'path';
import moment from 'moment';
import csvWriterPkg from 'csv-writer';

const { createObjectCsvWriter } = csvWriterPkg;

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
