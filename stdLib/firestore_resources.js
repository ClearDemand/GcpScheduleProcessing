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
    const tenants = await getMatchLibraryTenants();
    if (tenants.length === 0) return [];

    // tenant_code -> export_config, so the per-tenant export_config from
    // RetailScape can be merged onto the matching Firestore doc.
    const exportConfigByCode = new Map(tenants.map(t => [t.tenant_code, t.export_config]));
    const all = await getAllCompanyCodes();
    return all
        .filter(c => exportConfigByCode.has(c.company_code))
        .map(c => ({ ...c, export_config: exportConfigByCode.get(c.company_code) }));
}

// Tenants with the catalog sync-up feature enabled. Ported from
// PmtScheduleProcessing tpvr_resources.getClientsEnabledForCatalogSyncProcess:
// filter the company-code library on the `catalog_syncup_process` flag.
export async function getClientsEnabledForCatalogSyncProcess() {
    try {
        const allClients = await getAllCompanyCodes();
        return allClients.filter(client => !!client.catalog_syncup_process);
    } catch (error) {
        console.log(`getClientsEnabledForCatalogSyncProcess error: ${error.message}`);
        return [];
    }
}

// Competitor store mapping for a tenant, shaped like the old `domains` rows:
//   [{ source_store, client_specific_store_name, display }]
export async function getDomainsForClient(companyCode) {
    const data = await getCompanyDoc(companyCode);
    if (!data) return false;

    const banners = (data.competitor_banners || []).filter(b => b.is_banner_inactive !== true);
    return banners.map(b => ({
        source_store: b.key,
        client_specific_store_name: b.client_specific_store_name || b.display,
        display: b.display
    }));
}

// Full company-code doc for a tenant (banners, copy_brand_type, etc.), used by
// the match-update-processor job. GCP replacement for the old DynamoDB
// company_code_library.getCompanyKey(company_code). Returns false if the
// tenant has no company-code doc.
export async function getCompanyKey(companyCode) {
    return getCompanyDoc(companyCode);
}

async function getCompanyDoc(companyCode) {
    const snapshot = await firestore
        .collection(process.env.company_code_collection)
        .where('company_code', '==', companyCode)
        .get();

    if (snapshot.empty) return false;

    const data = snapshot.docs[0].data();
    data.competitor_banners = (data.competitor_banners || []).filter(b => b.is_banner_inactive !== true);
    return data;
}
