// ----------------------------------------------------------------------------
//  GCS resources (ES module) — output target (replaces S3 uploadObject).
//  Auth via Application Default Credentials (Cloud Run service account).
// ----------------------------------------------------------------------------
import { Storage } from '@google-cloud/storage';

const storage = new Storage({ projectId: process.env.GCP_PROJECT_ID });
const bucket = storage.bucket(process.env.gcs_bucket);

// Mirrors the shape of the old aws.uploadObject({ Key, Body }).
export async function uploadObject({ Key, Body }) {
    try {
        await bucket.file(Key).save(Body, { resumable: false });
        console.log(`Successfully uploaded data to GCS at ${Key}`);
        return true;
    } catch (err) {
        console.log(` -uploadObject Failed for ${Key}.\n${err.message}`);
        return false;
    }
}
