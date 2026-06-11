// ----------------------------------------------------------------------------
//  Firestore resources (ES module)
//
//  GCP replacement for the AWS DynamoDB calls match_library_export.js used:
//    - getLibraryExportClients()  was DynamoDB `CompanyCodeLibrary` scan
//    - getDomainsForClient()      was DynamoDB `domains` table; on GCP the
//      competitor store mapping lives in the doc's `competitor_banners`.
//
//  Connection mirrors matchlibrary-baas/src/lib/db/firestore.js (db
//  `matchlibrary-baas`).
// ----------------------------------------------------------------------------
import { Firestore } from '@google-cloud/firestore';
import { getMatchLibraryTenants } from './retailscape_resources.js';

const firestoreConfig = { projectId: process.env.GCP_PROJECT_ID };
if (process.env.FIRESTORE_DATABASE_ID && process.env.FIRESTORE_DATABASE_ID !== '(default)') {
    firestoreConfig.databaseId = process.env.FIRESTORE_DATABASE_ID;
}
const firestore = new Firestore(firestoreConfig);

// All company-code docs, with inactive competitor banners removed
// (mirrors PmtScheduleProcessing getAllCompanyCodes behaviour).
export async function getAllCompanyCodes() {
    const snapshot = await firestore.collection(process.env.company_code_collection).get();
    const items = [];
    snapshot.forEach(doc => {
        const data = doc.data();
        data.competitor_banners = (data.competitor_banners || []).filter(b => b.is_banner_inactive !== true);
        items.push(data);
    });
    return items;
}

// Tenants to export. The list of which tenants is driven by RetailScape's
// tenant_feature_v2 table (feature_name = 'match_library', is_enabled = true);
// the per-tenant config (banners / segments / flags) still lives in Firestore,
// so we return the Firestore docs for those tenant codes. tenant_code in
// RetailScape corresponds to company_code in Firestore.
// NOTE: retailscape_resources.init() must have run before this is called.
export async function getLibraryExportClients() {
    const tenantCodes = await getMatchLibraryTenants();
    if (tenantCodes.length === 0) return [];

    const wanted = new Set(tenantCodes);
    const all = await getAllCompanyCodes();
    return all.filter(c => wanted.has(c.company_code));
}

// Competitor store mapping for a tenant, shaped like the old `domains` rows:
//   [{ source_store, client_specific_store_name, display }]
export async function getDomainsForClient(companyCode) {
    const snapshot = await firestore
        .collection(process.env.company_code_collection)
        .where('company_code', '==', companyCode)
        .get();

    if (snapshot.empty) return false;

    const data = snapshot.docs[0].data();
    const banners = (data.competitor_banners || []).filter(b => b.is_banner_inactive !== true);

    return banners.map(b => ({
        source_store: b.key,
        client_specific_store_name: b.client_specific_store_name || b.display,
        display: b.display
    }));
}
