// ----------------------------------------------------------------------------
//  DynamoDB resources (ES module) — cross-cloud read of AWS's Global Product
//  Catalog (GPC). GPC has no GCP equivalent, so this Cloud Run Job reaches
//  into AWS directly using long-lived IAM credentials from GCP Secret Manager
//  (secretsManager.awsCrossCloud), same convention as aurora_resources.js.
//
//  Ported from PmtScheduleProcessing/stdLib/dynamo_resources.js#getGpcWithSku.
// ----------------------------------------------------------------------------
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { getSecretAsJson } from './secret_manager_resources.js';

const GPC_TABLE = 'GlobalProductCatalogv2';

let docClient;

export async function init() {
    const secretName = JSON.parse(process.env.secretsManager).awsCrossCloud;
    const cfg = await getSecretAsJson(secretName);

    const client = new DynamoDBClient({
        region: cfg.region || 'us-east-1',
        credentials: { accessKeyId: cfg.AWS_ACCESS_KEY_ID, secretAccessKey: cfg.AWS_SECRET_ACCESS_KEY }
    });
    docClient = DynamoDBDocumentClient.from(client);
    console.log(`DynamoDB (AWS cross-cloud) client ready (region=${cfg.region || 'us-east-1'})`);
}

// Authoritative GPC row for a comp_sku/comp_source_store pair. Multiple
// capture_date snapshots can exist per sku/store; returns the most recent one.
// Returns false if not found.
export async function getGpcWithSku(sku, source_store) {
    try {
        const { Items } = await docClient.send(new QueryCommand({
            TableName: GPC_TABLE,
            IndexName: 'sku-source_store-index',
            KeyConditionExpression: '#sku = :sku and #source_store = :source_store',
            ExpressionAttributeNames: { '#sku': 'sku', '#source_store': 'source_store' },
            ExpressionAttributeValues: { ':sku': sku, ':source_store': source_store }
        }));
        if (!Items || Items.length === 0) return false;
        return [...Items].sort((a, b) => (a.capture_date > b.capture_date ? 1 : -1)).at(-1);
    } catch (err) {
        console.log(`getGpcWithSku err: ${err.message}`);
        return false;
    }
}
