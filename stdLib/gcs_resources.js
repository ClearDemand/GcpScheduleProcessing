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

// V4 signed download URL, replacing the old s3.getSignedUrlPromise('getObject', ...).
// expiresInSeconds is SECONDS — the old code passed 1000*60*24*7 (a
// milliseconds-shaped number) into an API that treats Expires as seconds,
// so old links actually lived ~116 days instead of the intended ~7. GCS V4
// signed URLs also hard-cap at 7 days from creation.
// Requires the runtime SA to hold roles/iam.serviceAccountTokenCreator on
// itself (Cloud Run's keyless ADC signs via the IAM Credentials signBlob API).
export async function getSignedDownloadUrl(key, expiresInSeconds = 60 * 60 * 24 * 7) {
    try {
        const [url] = await bucket.file(key).getSignedUrl({
            version: 'v4',
            action: 'read',
            expires: Date.now() + expiresInSeconds * 1000
        });
        return url;
    } catch (err) {
        console.log(`getSignedDownloadUrl failed for ${key}.\n${err.message}`);
        return false;
    }
}
