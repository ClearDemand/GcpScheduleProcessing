// ----------------------------------------------------------------------------
//  Job: Match Library Export (GCP)
//
//
//    company-code library / competitor mapping : Firestore  (was DynamoDB)
//    matches / tenant_mapping / catalog date    : Cloud SQL  (was AWS Aurora)
//    output files                               : GCS bucket (was S3)
//
//  The transform logic (cleanMatches / cleanForExternalData / segments / zips)
//  is copied verbatim from the original.
// ----------------------------------------------------------------------------
import fs from 'fs';
import os from 'os';
import path from 'path';
import moment from 'moment';
import AdmZip from 'adm-zip';
import csvWriteStream from 'csv-write-stream';

import * as aurora from '../stdLib/aurora_resources.js';
import * as retailscape from '../stdLib/retailscape_resources.js';
import * as firestore from '../stdLib/firestore_resources.js';
import * as gcs from '../stdLib/gcs_resources.js';
import { createCsvFile, initExportFolder } from '../stdLib/node_utils.js';
import {
    notifySlackProcessStart,
    notifySlackProcessCompleted,
    notifySlackStatus
} from '../stdLib/sendSlackNotification.js';

const jobName = 'Match Library Export';

// Local staging dir for generated files before upload. Cloud Run's only
// writable path is the temp dir, so stage there.
const EXPORT_DIR = path.join(os.tmpdir(), 'MatchLibExport');
const stagePath = name => path.join(EXPORT_DIR, name);

const exportColumns = [
    'match_id', 'company_code', 'company_code_base_source_store', 'group_id', 'active',
    'client_sku_first_seen_date', 'matcher', 'match_date', 'match_time', 'match',
    'match_status', 'status', 'model_used', 'score', 'matcher_comments',
    'num_reviewers', 'reviewers', 'customer_review_state', 'bungee_review_state', 'previous_match',
    'base_source_store', 'base_upc', 'base_parent_sku', 'base_sku', 'base_price',
    'base_alt_price', 'base_custom_sku', 'base_url', 'base_img', 'base_brand', 'base_size',
    'base_title', 'base_category', 'base_subcategory', 'base_sub_subcategory', 'base_sub_sub_subcategory', 'base_sub_sub_sub_subcategory',
    'base_uom', 'base_alt_size', 'base_alt_uom', 'base_shipping_weight', 'base_dimensions',
    'comp_source_store', 'comp_upc', 'comp_parent_sku', 'comp_sku', 'comp_price',
    'comp_alt_price', 'comp_custom_sku', 'comp_url', 'comp_img',
    'comp_brand', 'comp_title', 'comp_category', 'comp_subcategory', 'comp_sub_subcategory',
    'comp_size', 'comp_uom', 'comp_alt_size', 'comp_alt_uom', 'comp_shipping_weight',
    'comp_dimensions', 'comp_store_name_display', 'base_store_name_display', 'bungee_verified_date',
    'customer_verified_date', 'manager_verified_date', 'internal_notes', 'converted_uom',
    'converted_size', 'conversion_user_id', 'conversion_timestamp',
    'tpvr_worker_choice', 'tpvr_worker', 'tpvr_manager_choice', 'tpvr_manager', 'comp_mfr_part_number',
    'base_subbrand', 'tpvr_reason_code', 'tpvr_reason_code_timestamp', 'updated_manager_verified_date',
    'suggested_by', 'base_brandtype', 'comp_brandtype', 'match_reason', 'base_pack_size', 'comp_pack_size',
    'base_total_size', 'comp_total_size', 'normalized_base_uom', 'normalized_comp_uom', 'normalized_comp_size',
    'normalized_comp_total_size', 'size_coefficient', 'quality_coefficient', 'size_quality_coefficient_reason', 'tpvr_rejection_comment', 'base_reg_price',
    'base_promo_price', 'comp_reg_price', 'comp_promo_price'
];

// Maps a competitor source_store -> display name. Built per-tenant from the
// company-code library's competitor_banners (banner `key` -> `display`)
// instead of a hardcoded list.
const buildStoreMap = (companyKey) =>
    Object.fromEntries((companyKey.competitor_banners || []).map((v) => [v.key, v.display]));

let globalZip = [];

// ------
//  Main
// ------
export async function run() {
    await aurora.init();
    await retailscape.init();

    const clients = await firestore.getLibraryExportClients();
    console.log(`${moment().format()} ${jobName} | ${clients.length} client(s) with match_library feature enabled in tenant_feature_v2 (ENV=${process.env.ENV || 'dev'})`);

    for (let i = 0; i < clients.length; i++) {
        await exportMatchLibrary(clients[i]);
    }
}

async function exportMatchLibrary(companyKey) {
    try {
        initExportFolder(EXPORT_DIR);

        // resetting global ZIP
        globalZip = [];

        const companyCode = companyKey.company_code;
        if (!companyKey.base_banners || !companyKey.competitor_banners) {
            return;
        }

        let tenantMappings = await aurora.getTenantMappings(companyCode);
        let baseBanners = companyKey.base_banners;
        let competitorBanners = companyKey.competitor_banners.map((v) => { return v.key });
        let competitorDisplayMap = Object.fromEntries(companyKey.competitor_banners.map((v) => [v.key, v.display]));
        if (baseBanners.length == 0 || competitorBanners.length == 0) {
            return;
        }
        await notifySlackProcessStart(jobName, companyCode);

        let competitorDomains = await firestore.getDomainsForClient(companyCode);
        if (!competitorDomains) {
            let message = 'The competitor mapping is not set up correctly for this tenant';
            await notifySlackProcessCompleted(jobName, companyCode, message);
            return;
        }

        let competitorMap = competitorDomains.reduce(function (map, obj) {
            map[`${obj.source_store}`] = obj;
            return map;
        }, {});

        let date = moment();
        let matchCount = 0;
        let partitionMatches = [];

        let internalFiles = new AdmZip();
        let internalZip = `all_matches_internal.zip`;
        let allInternalFilename = `all_matches_internal.csv`;

        let externalFiles = new AdmZip();
        let externalZip = `all_matches.zip`;
        let allExternalFilename = `all_matches.csv`;

        for (let b = 0; b < baseBanners.length; b++) {
            let segmentArr = [];
            let segments = companyKey.base_segments;
            if (segments) {
                segmentArr = getSegmentsForBanner(segments, baseBanners[b]);
            }
            if (segmentArr.length > 0) {
                for (let segment of segmentArr) {
                    await initializeZipSegment(segment, 'internal');
                    await initializeZipSegment(segment, 'external');
                }
            }

            let catalog_max_capture_date = await aurora.getLatestCatalogDate(companyCode, baseBanners[b]);
            catalog_max_capture_date = catalog_max_capture_date ? catalog_max_capture_date.max.split('-') : false;
            let maxCaptureDate = catalog_max_capture_date ? `${catalog_max_capture_date[0]}${catalog_max_capture_date[1]}${catalog_max_capture_date[2]}` : false;
            let baseName = `${baseBanners.length > 1 ? baseBanners[b].split('_')[1] : ''}`;

            for (let c = 0; c < competitorBanners.length; c++) {
                partitionMatches = await aurora.getMatchesByPartition(companyCode, baseBanners[b], competitorBanners[c], exportColumns, maxCaptureDate);
                if (partitionMatches && partitionMatches.length > 0) {
                    matchCount += partitionMatches.length;
                    const { internalMatches, externalMatches } = cleanMatches(partitionMatches, competitorMap, tenantMappings, companyKey);

                    if (internalMatches && internalMatches.length > 0) {
                        await consolidateMatches(allInternalFilename, internalMatches);

                        let internalFilename = `${baseName}${competitorBanners[c]}_matches_internal.csv`;
                        await createCsvFile(stagePath(internalFilename), internalMatches);

                        await gcs.uploadObject({
                            Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/internal/${internalFilename}`,
                            Body: fs.readFileSync(stagePath(internalFilename))
                        });

                        // segment file generation
                        if (companyKey.segment_view) {
                            for (let segment of segmentArr) {
                                let internalSegmentFilename = `${baseName}${competitorBanners[c]}_matches_${segment}_internal.csv`;
                                let segmentMatches = internalMatches.filter(matches => matches?.segment?.toLowerCase() == segment.toLowerCase());

                                if (segmentMatches && segmentMatches.length > 0) {
                                    await createCsvFile(stagePath(internalSegmentFilename), segmentMatches);

                                    await gcs.uploadObject({
                                        Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/internal/${internalSegmentFilename}`,
                                        Body: fs.readFileSync(stagePath(internalSegmentFilename))
                                    });

                                    let segmentZipName = getZipSegmentFile(segment, 'internal');
                                    if (segmentZipName) {
                                        await consolidateMatches(segmentZipName['allFileName'], segmentMatches);
                                    }
                                }
                            }
                        }
                    }

                    if (externalMatches && externalMatches.length > 0) {
                        await consolidateMatches(allExternalFilename, externalMatches);

                        let competitorName = competitorDisplayMap[competitorBanners[c]] || competitorBanners[c].split('_')[1];
                        let externalFilename = `${baseName}${competitorName}_matches.csv`;
                        await createCsvFile(stagePath(externalFilename), externalMatches);
                        await gcs.uploadObject({
                            Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/external/${externalFilename}`,
                            Body: fs.readFileSync(stagePath(externalFilename))
                        });

                        // segment file generation
                        if (companyKey.segment_view) {
                            for (let segment of segmentArr) {
                                let competitorNameSeg = competitorDisplayMap[competitorBanners[c]] || competitorBanners[c].split('_')[1];
                                let externalSegmentFileName = `${baseName}${competitorNameSeg}_matches_${segment}.csv`;
                                let segmentMatches = externalMatches.filter(matches => matches?.segment?.toLowerCase() == segment.toLowerCase());
                                if (segmentMatches && segmentMatches.length > 0) {
                                    await createCsvFile(stagePath(externalSegmentFileName), segmentMatches);

                                    await gcs.uploadObject({
                                        Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/external/${externalSegmentFileName}`,
                                        Body: fs.readFileSync(stagePath(externalSegmentFileName))
                                    });

                                    let segmentZipName = getZipSegmentFile(segment, 'external');
                                    if (segmentZipName) {
                                        await consolidateMatches(segmentZipName['allFileName'], segmentMatches);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        try {
            // Only present if this tenant produced internal matches; skip the
            // zip otherwise (nothing to consolidate).
            if (fs.existsSync(stagePath(allInternalFilename))) {
                internalFiles.addLocalFile(stagePath(allInternalFilename));
                await internalFiles.writeZipPromise(stagePath(internalZip));
                await gcs.uploadObject({
                    Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/internal/${internalZip}`,
                    Body: fs.readFileSync(stagePath(internalZip))
                });
            }
        } catch (err) {
            console.error(`Failed to add file: ${stagePath(allInternalFilename)}`, err.message);
        }
        // create all files for segment Matches
        if (companyKey.segment_view) {
            for (let zipSegment of globalZip) {
                if (zipSegment['type'] == 'internal') {
                    try {
                        if (fs.existsSync(stagePath(zipSegment['allFileName']))) {
                            let segmentZip = new AdmZip();
                            segmentZip.addLocalFile(stagePath(zipSegment['allFileName']));
                            await segmentZip.writeZipPromise(stagePath(zipSegment['zipFileName']));
                            await gcs.uploadObject({
                                Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/internal/${zipSegment['zipFileName']}`,
                                Body: fs.readFileSync(stagePath(zipSegment['zipFileName']))
                            });
                        }
                    } catch (err) {
                        console.error(`Failed to add file: ${stagePath(zipSegment['allFileName'])}`, err.message);
                    }
                }
            }
        }
        try {
            // Only present if this tenant produced external matches; skip the
            // zip otherwise (nothing to consolidate).
            if (fs.existsSync(stagePath(allExternalFilename))) {
                externalFiles.addLocalFile(stagePath(allExternalFilename));
                await externalFiles.writeZipPromise(stagePath(externalZip));
                await gcs.uploadObject({
                    Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/external/${externalZip}`,
                    Body: fs.readFileSync(stagePath(externalZip))
                });
            }
        } catch (err) {
            console.error(`Failed to add file: ${stagePath(allExternalFilename)}`, err.message);
        }
        // create all files for segment Matches
        if (companyKey.segment_view) {
            for (let zipSegment of globalZip) {
                if (zipSegment['type'] == 'external') {
                    try {
                        if (fs.existsSync(stagePath(zipSegment['allFileName']))) {
                            let segmentZip = new AdmZip();
                            segmentZip.addLocalFile(stagePath(zipSegment['allFileName']));
                            await segmentZip.writeZipPromise(stagePath(zipSegment['zipFileName']));
                            await gcs.uploadObject({
                                Key: `match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/external/${zipSegment['zipFileName']}`,
                                Body: fs.readFileSync(stagePath(zipSegment['zipFileName']))
                            });
                        }
                    } catch (err) {
                        console.error(`Failed to add file: ${stagePath(zipSegment['allFileName'])}`, err.message);
                    }
                }
            }
        }

        let msg = `Exported ${matchCount} Matches.\n`
            + `Generated files can be found at gs://${process.env.gcs_bucket}/match_library_export/company_code=${companyCode}/year=${date.format('YYYY')}/month=${date.format('MM')}/day=${date.format('DD')}/\n`;
        await notifySlackProcessCompleted(jobName, companyCode, msg);
    } catch (error) {
        console.log(`${moment().format()} - error in exportMatchLibrary: ${error}`);
        let errorMsg = `Export Match Library Job Crashed!\n`
            + `Error: ${error}`;
        await notifySlackStatus(jobName, 'ERROR', companyKey.company_code, errorMsg, true);
    }
}

function cleanMatches(matches, competitorMap, tenantMappings, companyKey) {
    try {
        let viewSet = new Set();
        let internalMatches = [], externalMatches = [];

        for (let i = 0; i < matches.length; i++) {
            let match = matches[i];

            let viewKey = `${match.base_source_store}_${match.base_sku}_${match.comp_sku}_${match.comp_source_store}`;
            if (!viewSet.has(viewKey)) {
                viewSet.add(viewKey);
                match.match_date = moment(match.manager_verified_date ?? match.customer_verified_date ?? moment.unix(match.match_date)).format('YYYY/MM/DD HH:mm');
                match.client_sku_first_seen_date = moment.unix(match.client_sku_first_seen_date).format('YYYY/MM/DD HH:mm');
                match.match_reason = fetchMatchReason(companyKey.equivalent_mappings, match.match_reason);
                if (match.company_code == 'chewy') {
                    let base_custom = parseObject(match.base_custom_attributes);
                    let comp_custom = parseObject(match.comp_custom_attributes);
                    delete match.base_custom_attributes;
                    delete match.comp_custom_attributes;
                    match.base_strength_concentration = base_custom && base_custom.base_strength_concentration ? base_custom.base_strength_concentration : null;
                    match.base_strength_concentration_uom = base_custom && base_custom.base_strength_concentration_uom ? base_custom.base_strength_concentration_uom : null;
                    match.comp_strength_concentration = comp_custom && comp_custom.comp_strength_concentration ? comp_custom.comp_strength_concentration : null;
                    match.comp_strength_concentration_uom = comp_custom && comp_custom.comp_strength_concentration_uom ? comp_custom.comp_strength_concentration_uom : null;
                    match.base_product_total_size = base_custom && base_custom.base_product_total_size ? base_custom.base_product_total_size : null;
                    match.base_product_total_uom = base_custom && base_custom.base_product_total_uom ? base_custom.base_product_total_uom : null;
                    match.comp_product_total_size = comp_custom && comp_custom.comp_product_total_size ? comp_custom.comp_product_total_size : null;
                    match.comp_product_total_uom = comp_custom && comp_custom.comp_product_total_uom ? comp_custom.comp_product_total_uom : null;
                    match.base_pharmacy_package_quantity = base_custom && base_custom.base_pharmacy_package_quantity ? base_custom.base_pharmacy_package_quantity : null;
                    match.base_pharmacy_package_quantity_uom = base_custom && base_custom.base_pharmacy_package_quantity_uom ? base_custom.base_pharmacy_package_quantity_uom : null;
                    match.comp_pharmacy_package_quantity = comp_custom && comp_custom.comp_pharmacy_package_quantity ? comp_custom.comp_pharmacy_package_quantity : null;
                    match.comp_pharmacy_package_quantity_uom = comp_custom && comp_custom.comp_pharmacy_package_quantity ? comp_custom.comp_pharmacy_package_quantity : null;
                    match.base_total_quantity = base_custom && base_custom.base_total_quantity ? base_custom.base_total_quantity : null;
                    match.base_total_quantity_uom = base_custom && base_custom.base_total_quantity_uom ? base_custom.base_total_quantity_uom : null;
                    match.comp_total_quantity = comp_custom && comp_custom.comp_total_quantity ? comp_custom.comp_total_quantity : null;
                    match.comp_total_quantity_uom = comp_custom && comp_custom.comp_total_quantity_uom ? comp_custom.comp_total_quantity_uom : null;
                    match.comp_converted_quantity = comp_custom && comp_custom.comp_converted_quantity ? comp_custom.comp_converted_quantity : null;
                    match.comp_converted_quantity_uom_normalized = comp_custom && comp_custom.comp_converted_quantity_uom_normalized ? comp_custom.comp_converted_quantity_uom_normalized : null;
                }

                let internalData = { ...match };
                internalMatches.push(internalData);

                let externalData = cleanForExternalData(match, competitorMap, tenantMappings, companyKey);
                if (companyKey.company_code == 'ctc') {
                    if (match.active == true) {
                        externalMatches.push(externalData);
                    }
                } else {
                    externalMatches.push(externalData);
                }
            }
        }

        return { internalMatches, externalMatches };
    } catch (error) {
        console.log(error);
        return { internalMatches: [], externalMatches: [] };
    }
}

function cleanForExternalData(data, competitorMap, tenantMappings, companyKey) {
    try {
        let externalData = {};
        externalData[`active`] = data.active;
        externalData[`match_date`] = data.match_date;
        externalData[`match_type`] = data.match;
        externalData[`tpvr_worker`] = data.tpvr_worker;
        externalData[`tpvr_manager`] = data.tpvr_manager;
        const matcherStr = typeof data.matcher === 'string' ? data.matcher.trim() : '';
        const isEmail = (str) => str.length > 0 && /\S+@\S+\.\S+/.test(str);
        if (isEmail(matcherStr) && !matcherStr.toLowerCase().includes('cleardemand') && !matcherStr.toLowerCase().includes('bungee')) {
            externalData['uploaded_by'] = matcherStr;
        } else {
            externalData['uploaded_by'] = null;
        }

        externalData[`base`] = data.base_source_store.split('_')[1];
        externalData[`base_sku`] = `\t${data.base_sku}`;
        externalData[`base_upc`] = `\t${data.base_upc}`;
        if (companyKey.company_code == 'ctc') {
            externalData['Style ID'] = data.base_parent_sku;
        }
        externalData[`base_title`] = data.base_title;
        externalData[`base_url`] = data.base_url;
        externalData[`base_size`] = data.base_size;
        externalData[`base_uom`] = data.base_uom;
        if (companyKey.uom_normalization_flag_enabled) {
            externalData['normalized_base_uom'] = data.normalized_base_uom;
        }
        externalData['base_pack_size'] = data.base_pack_size;
        externalData['base_total_size'] = data.base_total_size;
        if (companyKey.company_code == 'ctc') {
            externalData['base_reg_price'] = data.base_price;
            externalData['base_promo_price'] = data.base_promo_price;
        }

        externalData[`base_${tenantMappings.category}`] = data.base_category;
        externalData[`base_${tenantMappings.subcategory}`] = data.base_subcategory;
        externalData[`base_${tenantMappings.sub_subcategory}`] = data.base_sub_subcategory;
        if (companyKey.company_code == 'ctc') {
            externalData[`base_${tenantMappings.sub_sub_subcategory}`] = data.base_sub_sub_subcategory;
            externalData[`base_${tenantMappings.sub_sub_sub_subcategory}`] = data.base_sub_sub_sub_subcategory;
        }
        externalData[`competitor`] = (competitorMap[data.comp_source_store] || {})['client_specific_store_name'] || data.comp_source_store.split('_')[1];
        if (['wholefoods', 'wfuk', 'wfca'].includes(data.company_code)) {
            const storeMap = buildStoreMap(companyKey);
            externalData[`competitor_name`] = storeMap[data.comp_source_store] || (competitorMap[data.comp_source_store] || {})['display'] || data.comp_source_store.split('_')[1];
        }
        externalData[`comp_sku`] = `\t${data.comp_custom_sku || data.comp_sku}`;
        externalData[`comp_upc`] = `\t${data.comp_upc}`;

        externalData[`comp_title`] = data.comp_title?.replace(/[^a-zA-Z0-9 &-]/g, '');
        externalData[`comp_url`] = data.comp_url;
        externalData[`comp_size`] = data.comp_size;
        externalData[`comp_uom`] = data.comp_uom;
        if (companyKey.pack_size_enabled) {
            externalData['comp_pack_size'] = data.comp_pack_size;
            externalData['comp_total_size'] = data.comp_total_size;
        }

        if (companyKey.uom_normalization_flag_enabled) {
            externalData['normalized_comp_uom'] = data.normalized_comp_uom;
            externalData['normalized_comp_size'] = data.normalized_comp_size;
            externalData['normalized_comp_total_size'] = data.normalized_comp_total_size;
        }
        if (companyKey.company_code == 'ctc') {
            externalData['comp_reg_price'] = data.comp_reg_price;
            externalData['comp_promo_price'] = data.comp_promo_price;
        }
        externalData[`comp_${tenantMappings.category}`] = data.comp_category;
        externalData[`comp_${tenantMappings.subcategory}`] = data.comp_subcategory;
        externalData[`comp_${tenantMappings.sub_subcategory}`] = data.comp_sub_subcategory;
        externalData[`bungee_review_state`] = capitalizeFirstLetter(data.bungee_review_state);
        externalData[`match_reason`] = data.match_reason;
        externalData[`segment`] = data.segment || '';
        if (data.customer_review_state == 'unverified') {
            externalData[`customer_review_state`] = 'Unverified';
        } else if (data.customer_review_state == 'customer_verified') {
            externalData[`customer_review_state`] = 'Worker Verified';
        } else if (data.customer_review_state == 'both_customer_manager_verified') {
            externalData[`customer_review_state`] = 'Manager Verified';
        } else {
            externalData[`customer_review_state`] = data.customer_review_state;
        }
        if (['wholefoods', 'wfuk', 'wfca'].includes(data.company_code)) {
            if (data.customer_review_state == 'customer_verified') {
                externalData[`customer_review_state`] = externalData[`match_type`] === 'reference' ? 'Worker Verified Reference' : 'Worker Verified';
                if (data.customer_verified_date) {
                    externalData[`worker_review_timestamp`] = moment(data.customer_verified_date).format('YYYY-MM-DD HH:mm') || null;
                }
            } else if (data.customer_review_state == 'both_customer_manager_verified') {
                externalData[`customer_review_state`] = externalData[`match_type`] === 'reference' ? 'Manager Verified Reference' : 'Manager Verified';
                if (data.manager_verified_date) {
                    externalData[`worker_review_timestamp`] = moment(data.manager_verified_date).format('YYYY-MM-DD HH:mm') || null;
                }
            }
        }
        if (companyKey.verification_queue_enabled && !['wholefoods', 'wfuk', 'wfca'].includes(data.company_code)) {
            externalData[`customer_verified_date`] = null;
            externalData[`manager_verified_date`] = null;
            if (data.customer_review_state == 'unverified') {
                externalData[`customer_review_state`] = 'Unverified';
            } else if ((data.customer_review_state == 'customer_verified' || data.customer_review_state == 'both_customer_manager_verified')) {
                externalData[`customer_review_state`] = 'Worker Verified';
                if (data.customer_verified_date) {
                    externalData[`customer_verified_date`] = moment(data.customer_verified_date).format('YYYY-MM-DD');
                }
            }
            if (data.customer_review_state == 'both_customer_manager_verified' && data.tpvr_worker_choice === 'verify_match') {
                externalData[`customer_review_state`] = 'Manager Verified';
                if (data.manager_verified_date) {
                    externalData[`manager_verified_date`] = moment(data.manager_verified_date).format('YYYY-MM-DD');
                }
            }
        }
        if (data.company_code == 'ctc') {
            externalData['size_coefficient'] = data.size_coefficient;
            externalData['quality_coefficient'] = data.quality_coefficient;
            externalData['size_quality_coefficient_reason'] = data.size_quality_coefficient_reason;
        }
        if (companyKey.tpvr_comment_enabled) {
            externalData['tpvr_comment'] = data.tpvr_rejection_comment;
        }
        if (data.company_code == 'chewy') {
            externalData[`base_strength`] = data.base_strength_concentration;
            externalData[`base_strength_uom`] = data.base_strength_concentration_uom;
            externalData[`comp_strength`] = data.comp_strength_concentration;
            externalData[`comp_strength_uom`] = data.comp_strength_concentration_uom;
            externalData[`base_product_total_size`] = data.base_product_total_size;
            externalData[`base_product_total_uom`] = data.base_product_total_uom;
            externalData[`comp_product_total_size`] = data.comp_product_total_size;
            externalData[`comp_product_total_uom`] = data.comp_product_total_uom;
            externalData[`base_quantity`] = data.base_pharmacy_package_quantity;
            externalData[`base_quantity_uom`] = data.base_pharmacy_package_quantity_uom;
            externalData[`comp_quantity`] = data.comp_pharmacy_package_quantity;
            externalData[`comp_quantity_uom`] = data.comp_pharmacy_package_quantity_uom;
            externalData[`base_total_quantity`] = data.base_total_quantity;
            externalData[`base_total_quantity_uom`] = data.base_total_quantity_uom;
            externalData[`comp_total_quantity`] = data.comp_total_quantity;
            externalData[`comp_total_quantity_uom`] = data.comp_total_quantity_uom;
            externalData[`base_normalized_size`] = data.base_size;
            externalData[`base_normalized_uom`] = data.base_uom;
            externalData[`comp_normalized_size`] = data.comp_converted_quantity;
            externalData[`comp_normalized_uom`] = data.comp_converted_quantity_uom_normalized;
        }

        return externalData;
    } catch (error) {
        console.log(error);
        return false;
    }
}

function capitalizeFirstLetter(string) {
    try {
        if (!string) {
            return '';
        }
        return string.charAt(0).toUpperCase() + string.slice(1);
    } catch (error) {
        console.log(error);
        return string;
    }
}

// Appends `data` rows to the consolidated CSV at `fileName`. Returns a promise
// that resolves only once the append has fully flushed to disk — callers must
// await it, otherwise the later addLocalFile() can read a missing/truncated
// file (and concurrent append streams to the same file can interleave).
function consolidateMatches(fileName, data) {
    return new Promise((resolve) => {
        try {
            if (!data || data.length === 0) {
                return resolve();
            }
            let writer;
            let filePath = stagePath(fileName);

            if (!fs.existsSync(filePath)) {
                let headers = Object.keys(data[0]).map((v) => { return v; });
                writer = csvWriteStream({ headers });
            } else {
                writer = csvWriteStream({ sendHeaders: false });
            }
            const out = fs.createWriteStream(filePath, { flags: 'a' });
            out.on('finish', resolve);
            out.on('error', (error) => { console.log(error); resolve(); });
            writer.pipe(out);

            for (let i = 0; i < data.length; i++) {
                writer.write(data[i]);
            }
            writer.end();
        } catch (error) {
            console.log(error);
            resolve();
        }
    });
}

function parseObject(inputObject) {
    if (!inputObject) {
        return null;
    }
    const parsedObject = {};

    for (const key in inputObject) {
        let value = inputObject[key];

        // Convert specific strings to null
        if (value === "" || value === "NA" || value === "null" || value === "n/a" || value === " ") {
            value = null;
        }

        parsedObject[key] = value;
    }

    return parsedObject;
}

function fetchMatchReason(equivalentMappings, matchReason) {
    if (!matchReason) {
        return "";
    }
    if (!equivalentMappings) {
        return matchReason;
    }
    for (let i = 0; i < equivalentMappings.length; i++) {
        if (equivalentMappings[i].value == matchReason) {
            return equivalentMappings[i].modified_key.toLowerCase();
        }
    }
}

function getSegmentsForBanner(segments, bannerKey) {
    if (!segments || segments.length === 0) return [];

    for (let segment of segments) {
        for (let innerSegment of Object.keys(segment)) {
            if (innerSegment === bannerKey) {
                return [...segment[innerSegment]];
            }
        }
    }

    return [];
}

const initializeZipSegment = async (segmentName, type) => {
    const zipFileName = `all_${segmentName}_${type}_matches.zip`;
    const allFileName = `all_${segmentName}_${type}_matches.csv`;

    globalZip.push({
        segment: segmentName,
        type: type,
        zipFileName: zipFileName,
        allFileName: allFileName
    });
};

const getZipSegmentFile = (segmentName, type) => {
    for (let segmentZips of globalZip) {
        if (segmentZips['segment'] == segmentName && segmentZips['type'] == type) {
            return segmentZips;
        }
    }
    return false;
};
