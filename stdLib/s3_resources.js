// ----------------------------------------------------------------------------
//  S3 resources (ES module) — output target for auto-ingestion reports.
//  Cross-cloud AWS credentials, same convention as athena_resources.js /
//  dynamo_resources.js (secretsManager.awsCrossCloud).
//
//  Ported from PmtScheduleProcessing/stdLib/aws_resources.js#uploadObject and
//  scripts/upc_matches_auto_approval.js's
//  s3.getSignedUrlPromise('getObject', ...) — same bucket
//  (bungee.productmatching) Athena already writes its query output to.
// ----------------------------------------------------------------------------
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { getSecretAsJson } from './secret_manager_resources.js';

const BUCKET = 'bungee.productmatching';

let client;

export async function init() {
    const secretName = JSON.parse(process.env.secretsManager).awsCrossCloud;
    const cfg = await getSecretAsJson(secretName);

    client = new S3Client({
        region: cfg.region || 'us-east-1',
        credentials: { accessKeyId: cfg.AWS_ACCESS_KEY_ID, secretAccessKey: cfg.AWS_SECRET_ACCESS_KEY }
    });
    console.log(`S3 (AWS cross-cloud) client ready (region=${cfg.region || 'us-east-1'})`);
}

// Mirrors the shape of the old aws.uploadObject({ Key, Body }) / gcs.uploadObject.
export async function uploadObject({ Key, Body }) {
    try {
        await client.send(new PutObjectCommand({ Bucket: BUCKET, Key, Body }));
        console.log(`Successfully uploaded data to S3 at ${Key}`);
        return true;
    } catch (err) {
        console.log(` -uploadObject Failed for ${Key}.\n${err.message}`);
        return false;
    }
}

// SigV4 presigned URL, replacing the old s3.getSignedUrlPromise('getObject', ...).
// 7 days (604800s) is the max expiry AWS allows for a presigned URL.
export async function getSignedDownloadUrl(key, expiresInSeconds = 60 * 60 * 24 * 7) {
    try {
        return await getSignedUrl(client, new GetObjectCommand({ Bucket: BUCKET, Key: key }), { expiresIn: expiresInSeconds });
    } catch (err) {
        console.log(`getSignedDownloadUrl failed for ${key}.\n${err.message}`);
        return false;
    }
}
