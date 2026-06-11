// ----------------------------------------------------------------------------
//  GCP Secret Manager (ES module)
//  Mirrors matchlibrary-baas/src/lib/gcp-api/secretManager.js.
// ----------------------------------------------------------------------------
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';

const client = new SecretManagerServiceClient();

// Fetches a secret by name and parses its payload as JSON.
export async function getSecretAsJson(secretName) {
    const name = `projects/${process.env.GCP_PROJECT_ID}/secrets/${secretName}/versions/latest`;
    const [version] = await client.accessSecretVersion({ name });
    return JSON.parse(version.payload.data.toString());
}
