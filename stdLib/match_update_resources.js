// ----------------------------------------------------------------------------
//  Match Update Processor resources (ES module)
//
//  Ported from PmtScheduleProcessing/poll.js + stdLib/match_update_resources.js
//  — queue claiming, competitor-match lookups, and the per-match sync logic
//  that reconciles a match row against an authoritative source (DynamoDB's
//  Global Product Catalog, or the Athena pricing-parquet dataset).
//
//  Queries are parameterized (runQuery(text, values)) instead of the old
//  raw string interpolation. Sentinel "no value" strings and the comp_*
//  attribute key maps live in match_update_constants.js (the old code had
//  3-4 inconsistent copies of the sentinel list — consolidated to one here).
// ----------------------------------------------------------------------------
import * as aurora from './aurora_resources.js';
import * as dynamo from './dynamo_resources.js';
import {
    isSentinelValue,
    GPC_KEY_MAP,
    GPC_CUSTOM_ATTRIBUTE_KEYS,
    PRICING_PARQUET_CUSTOM_ATTRIBUTE_KEYS
} from './match_update_constants.js';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// ------------------------
//  Queue claim / status
// ------------------------

// Atomically claims the oldest pending group (all matches_update_queue rows
// sharing a process_id) and flips it to 'processing'. Safe under concurrent
// trigger-per-enqueue invocations: FOR UPDATE SKIP LOCKED lets a losing
// claimant move on to a different candidate row instead of blocking, and the
// outer UPDATE's WHERE ingestion_status = 'pending' means a claimant that
// collides with another sibling row of the same process_id gets back an
// empty result (no double-claim) rather than an error. An empty result can
// legitimately mean "lost a race, try again" rather than "queue empty", so
// this retries until the queue is confirmed empty.
export async function claimNextPendingGroup(maxAttempts = 5) {
    const claimQuery = `
        UPDATE matches_update_queue
        SET ingestion_status = 'processing'::match_update_status_enum, updated_timestamp = NOW()
        WHERE process_id = (
            SELECT process_id FROM matches_update_queue
            WHERE ingestion_status = 'pending'
            ORDER BY ingested_timestamp ASC LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        AND ingestion_status = 'pending'
        RETURNING *;
    `;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const rows = await aurora.runQuery(claimQuery);
        if (rows && rows.length > 0) return rows;

        const remaining = await aurora.runQuery(
            `SELECT 1 FROM matches_update_queue WHERE ingestion_status = 'pending' LIMIT 1`
        );
        if (!remaining || remaining.length === 0) return [];
        await sleep(75 * attempt);
    }
    return [];
}

// Marks every matches_update_queue row for a process_id with an explicit
// status. Old updateMatchesQueueTable/updateAllPendingListingsInQueueTable
// both hardcoded ingestion_status = 'completed' even on failure paths (only
// varying the comment) — that bug is not replicated here; status is explicit.
export async function markGroupStatus(companyCode, processId, status, statusComment) {
    return aurora.runQuery(
        `UPDATE matches_update_queue_${companyCode}
         SET ingestion_status = $3::match_update_status_enum, status_comments = $4, updated_timestamp = NOW()
         WHERE company_code = $1 AND process_id = $2
         RETURNING *;`,
        [companyCode, processId, status, statusComment]
    );
}

// Bulk per-row status update for the non-'competitor' update_level path,
// where each matches_update_queue row IS one match to sync (identified by
// its own `id`), so completion is tracked per-row instead of per-group.
export async function updatePendingListingStatuses(companyCode, updates) {
    if (!updates || updates.length === 0) return [];

    const rows = updates.map((_, i) => `($${i * 3 + 1}::uuid, $${i * 3 + 2}::match_update_status_enum, $${i * 3 + 3})`);
    const values = updates.flatMap(u => [u.id, u.status, u.statusComments ?? null]);

    return aurora.runQuery(
        `UPDATE matches_update_queue_${companyCode} AS q
         SET ingestion_status = c.status, status_comments = c.status_comments, updated_timestamp = NOW()
         FROM (VALUES ${rows.join(', ')}) AS c(id, status, status_comments)
         WHERE q.id = c.id;`,
        values
    );
}

// ------------------------
//  Match lookups
// ------------------------

// Matches for a company/base/competitor partition, optionally filtered by
// active status or a match_date window. Ported from the old same-named
// function, parameterized instead of raw string interpolation.
export async function getAllCompetitorMatches(companyCode, baseSourceStore, compSourceStore, active, inactiveOnly, startDate, endDate) {
    try {
        const table = `matches_${companyCode}_${baseSourceStore}`;
        let query = `SELECT * FROM ${table} WHERE LOWER(comp_source_store) = LOWER($1)`;
        const values = [compSourceStore];

        if (active) {
            query += ` AND active = true`;
        } else if (inactiveOnly) {
            query += ` AND active = false AND deleted_date is null AND match_status = 'product_found'`;
        }
        if (startDate && startDate.length > 0) {
            query += ` AND match_date BETWEEN EXTRACT(EPOCH FROM $${values.length + 1}::timestamp)::INTEGER
                        AND EXTRACT(EPOCH FROM $${values.length + 2}::timestamp)::INTEGER`;
            values.push(`${startDate} 00:00:00`, `${endDate || startDate} 23:59:59`);
        }

        return await aurora.runQuery(query, values);
    } catch (error) {
        console.log(`getAllCompetitorMatches err: ${error.message}`);
        return [];
    }
}

// Ported from the old same-named function, parameterized.
export async function getMatchWithMatchId(match) {
    try {
        const table = `matches_${match.company_code}_${match.base_source_store}`;
        const rows = await aurora.runQuery(`SELECT * FROM ${table} WHERE match_id = $1`, [match.match_id]);
        return rows[0];
    } catch (error) {
        console.log(`getMatchWithMatchId err: ${error.message}`);
        throw error;
    }
}

// ------------------------
//  ctc/amazonca special case (shared by GPC + pricing-parquet paths)
// ------------------------

// title > size_attribute > item_weight precedence. Returns null if no usable
// value. Ported from poll.js's extractAmazoncaPackValue.
function extractAmazoncaPackValue(allPackSizes, valueField) {
    if (!allPackSizes) return null;
    for (const field of ['title', 'size_attribute', 'item_weight']) {
        const entry = allPackSizes[field];
        if (entry && entry[valueField] != null) {
            const val = `${entry[valueField]}`.trim();
            if (val && !isSentinelValue(val)) return val;
        }
    }
    return null;
}

function getAmazoncaValueFromExtraDetails(gpcData, valueField) {
    return extractAmazoncaPackValue(gpcData?.extraDetails?.all_pack_sizes, valueField);
}

function getAmazoncaValueFromPricingRow(pricingRow, valueField) {
    let allPackSizes = null;
    try { allPackSizes = pricingRow?.all_pack_sizes ? JSON.parse(pricingRow.all_pack_sizes) : null; } catch { /* not valid JSON, treat as absent */ }
    return extractAmazoncaPackValue(allPackSizes, valueField);
}

// ------------------------
//  GPC (DynamoDB) sync path
// ------------------------

// Syncs one match's comp_* attributes from GPC. Returns the input `data`
// object augmented with `.processed` ({modified, status}), `.updated`
// (flattened before/after fields for the report), and `.auditData`
// ({prev_value, new_value}) when a change was made.
export async function syncRecords(data, keysToUpdate, companyKey) {
    try {
        data.processed = {};
        data.updated = {};

        if (!data.comp_sku) {
            data.processed.modified = false;
            data.processed.status = 'Sku Missing';
            return data;
        }

        const gpcData = await dynamo.getGpcWithSku(data.comp_sku?.toLowerCase(), data.comp_source_store);
        const matchData = await getMatchWithMatchId(data);
        const dqIssue = validateData(matchData, gpcData, keysToUpdate, data.company_code, data.comp_source_store);

        if (dqIssue === 'auroraMissing') {
            data.processed.modified = false;
            data.processed.status = 'Aurora Data Missing';
            return data;
        } else if (dqIssue === 'missing') {
            data.processed.modified = false;
            data.processed.status = 'GPC Data Missing';
            return data;
        } else if (dqIssue === 'error') {
            data.processed.modified = false;
            data.processed.isError = true;
            data.processed.status = 'Error in Processing Updation of GPC Data';
            return data;
        } else if (dqIssue === 'none') {
            data.processed.modified = false;
            data.processed.status = 'No Changes Detected';
            return data;
        }

        const modifyValues = {};
        const auditData = { prev_value: {}, new_value: {} };

        for (const key of keysToUpdate) {
            if (key === 'comp_custom_attributes') {
                if (checkCompCustomAttributes(gpcData, matchData.comp_custom_attributes)) {
                    auditData.prev_value.comp_custom_attributes = matchData.comp_custom_attributes;
                    fillCompCustomAttributes(modifyValues, gpcData);
                    auditData.new_value.comp_custom_attributes = modifyValues.comp_custom_attributes;
                }
                continue;
            }

            let gpcValue = gpcData[GPC_KEY_MAP[key].val];
            const auroraVal = `${(matchData[key] || '')}`.trim().toLowerCase();

            if (key === 'comp_pack_size') {
                gpcValue = gpcValue?.split(' ')[0] || '';
            }
            if (key === 'comp_size' && data.company_code === 'ctc' && data.comp_source_store?.includes('amazonca')) {
                const sizeFromExtra = getAmazoncaValueFromExtraDetails(gpcData, 'size');
                if (sizeFromExtra !== null) gpcValue = sizeFromExtra;
            }
            if (key === 'comp_uom' && data.company_code === 'ctc' && data.comp_source_store?.includes('amazonca')) {
                const uomFromExtra = getAmazoncaValueFromExtraDetails(gpcData, 'uom');
                if (uomFromExtra !== null) gpcValue = uomFromExtra;
            }

            const gpcVal = `${(gpcValue || '')}`.trim().toLowerCase();

            if (gpcValue != null && gpcValue !== '-' && gpcValue !== '' && gpcVal !== auroraVal && !isSentinelValue(gpcVal)) {
                auditData.prev_value[key] = matchData[key];
                modifyValues[key] = GPC_KEY_MAP[key].type === 'string' ? `${gpcValue}` : gpcValue;
                auditData.new_value[key] = modifyValues[key];
            } else {
                modifyValues[key] = matchData[key];
            }

            if (key === 'comp_brand') {
                const compBrandValue = modifyValues[key] || matchData.comp_brand;
                if (compBrandValue && !isSentinelValue(compBrandValue) && matchData.comp_source_store) {
                    auditData.prev_value.comp_brandtype = matchData.comp_brandtype;
                    const brandType = await aurora.getCompBrandType(matchData.comp_source_store.toLowerCase(), compBrandValue?.toLowerCase());
                    modifyValues.comp_brandtype = brandType || 'NB';
                }
                if (companyKey.copy_brand_type && !modifyValues.comp_brandtype && isSentinelValue(modifyValues[key])) {
                    modifyValues.comp_brandtype = matchData.base_brandtype;
                }
                auditData.new_value.comp_brandtype = modifyValues.comp_brandtype;
            }

            if (key === 'comp_pack_size') {
                const compPackSize = modifyValues[key] || matchData.comp_pack_size || 1;
                const compSize = modifyValues.comp_size || matchData.comp_size || 1;
                const normalizedCompSize = matchData.normalized_comp_size || 1;
                if (compPackSize && !isSentinelValue(compPackSize)) {
                    auditData.prev_value.comp_total_size = matchData.comp_total_size;
                    auditData.prev_value.normalized_comp_total_size = matchData.normalized_comp_total_size;

                    const totalSize = (compPackSize * compSize) || 1;
                    modifyValues.comp_total_size = totalSize.toString();
                    const normalizedCompTotalSize = (compPackSize * normalizedCompSize) || 1;
                    modifyValues.normalized_comp_total_size = normalizedCompTotalSize.toString();

                    auditData.new_value.comp_total_size = totalSize.toString();
                    auditData.new_value.normalized_comp_total_size = normalizedCompTotalSize.toString();
                }
            }
        }

        if (matchData.active) {
            modifyValues.updated_manager_verified_date = new Date().toISOString();
        }

        const resp = await aurora.updateMatchAttributes(data.company_code, data.base_source_store, data.comp_source_store, matchData.match_id, modifyValues);
        if (resp) {
            data.processed.response = resp;
            data.processed.modified = true;
            data.processed.status = 'Successfully Updated to DB';
        }

        for (const [k, v] of Object.entries(modifyValues)) {
            data.updated[`updated_${k}`] = v;
        }
        for (const key of keysToUpdate) {
            addToUpdatedField(matchData, data, key);
            if (key === 'comp_brand') addToUpdatedField(matchData, data, 'comp_brandtype');
        }

        data.auditData = auditData;
        return data;
    } catch (error) {
        console.log(`syncRecords error for match_id=${data?.match_id}: ${error.message}`);
        data.processed = { modified: false, isError: true, status: `Error: ${error.message}` };
        data.updated = data.updated || {};
        return data;
    }
}

function validateData(auroraData, gpcData, keysToUpdate, companyCode, compSourceStore) {
    try {
        if (!gpcData) return 'missing';
        if (!auroraData) return 'auroraMissing';

        for (const auroraKey of keysToUpdate) {
            if (auroraKey === 'comp_custom_attributes') {
                if (checkCompCustomAttributes(gpcData, auroraData.comp_custom_attributes)) return 'detected';
                continue;
            }

            if ((auroraKey === 'comp_size' || auroraKey === 'comp_uom') && companyCode === 'ctc' && compSourceStore?.includes('amazonca')) {
                const extraVal = auroraKey === 'comp_size'
                    ? getAmazoncaValueFromExtraDetails(gpcData, 'size')
                    : getAmazoncaValueFromExtraDetails(gpcData, 'uom');
                if (extraVal !== null) {
                    const auroraVal = `${(auroraData[auroraKey] || '')}`.toLowerCase();
                    const lowerExtra = extraVal.toLowerCase();
                    if (lowerExtra !== '' && lowerExtra !== '-' && auroraVal !== lowerExtra) return 'detected';
                    continue;
                }
                // fall through to the standard GPC key check
            }

            const gpcKey = GPC_KEY_MAP[auroraKey].val;
            if (gpcData[gpcKey]) {
                const auroraVal = `${(auroraData[auroraKey] || '')}`.toLowerCase();
                let gpcVal = `${(gpcData[gpcKey] || '')}`.toLowerCase();
                if (auroraKey === 'comp_pack_size') gpcVal = gpcVal?.split(' ')[0] || '';
                if (gpcVal !== '' && gpcVal !== '-' && auroraVal !== gpcVal) return 'detected';
            }
        }
        return 'none';
    } catch (error) {
        console.log(error);
        return 'error';
    }
}

function checkCompCustomAttributes(gpcData, compCustomAttributes) {
    try {
        for (const [key, def] of Object.entries(GPC_CUSTOM_ATTRIBUTE_KEYS)) {
            if (def.level === '1') {
                const auroraVal = compCustomAttributes && compCustomAttributes[key];
                const gpcVal = gpcData && gpcData[def.val];
                if (gpcVal && !isSentinelValue(gpcVal) && gpcVal != auroraVal) return true;
            }
            if (gpcData.pharmacy && def.level === '2') {
                const auroraVal = compCustomAttributes && compCustomAttributes[key];
                const gpcVal = gpcData.pharmacy[def.val];
                if (gpcVal && !isSentinelValue(gpcVal) && gpcVal != auroraVal) return true;
            }
        }
        return false;
    } catch (error) {
        console.log('checkCompCustomAttributes error', error);
        return false;
    }
}

function fillCompCustomAttributes(modifyValues, gpcData) {
    let newCustomAttribute = {
        comp_total_quantity: null, comp_product_total_uom: null, comp_product_total_size: null,
        comp_total_quantity_uom: null, comp_strength_concentration: null, comp_pharmacy_package_quantity: null,
        comp_strength_concentration_uom: null, comp_pharmacy_package_quantity_uom: null
    };
    newCustomAttribute.comp_product_total_uom = gpcData.product_total_uom || null;
    newCustomAttribute.comp_product_total_size = gpcData.product_total_size || null;
    if (isSentinelValue(newCustomAttribute.comp_product_total_uom) || isSentinelValue(newCustomAttribute.comp_product_total_size)) {
        newCustomAttribute.comp_product_total_uom = null;
        newCustomAttribute.comp_product_total_size = null;
    }
    if (gpcData.pharmacy) {
        newCustomAttribute.comp_strength_concentration = gpcData.pharmacy.strength_concentration || null;
        newCustomAttribute.comp_strength_concentration_uom = gpcData.pharmacy.strength_concentration_uom || null;
        newCustomAttribute.comp_pharmacy_package_quantity = gpcData.pharmacy.pharmacy_package_quantity || null;
        newCustomAttribute.comp_pharmacy_package_quantity_uom = gpcData.pharmacy.pharmacy_package_quantity_uom || null;
        newCustomAttribute.comp_total_quantity = gpcData.pharmacy.package_quantity || null;
        newCustomAttribute.comp_total_quantity_uom = gpcData.pharmacy.package_quantity_uom || null;
    }
    // Old code JSON.stringify'd newCustomAttribute here only inside the
    // `if (gpcData.pharmacy)` branch, leaving it a plain object otherwise —
    // an inconsistency that would double-encode the JSON when it did fire
    // (pg auto-serializes plain objects for jsonb columns). Always leave it
    // as an object; runQuery's parameter binding handles the jsonb cast.
    modifyValues.comp_custom_attributes = newCustomAttribute;
}

function addToUpdatedField(matchData, data, key) {
    try {
        let value = matchData[key];
        if (value && typeof value === 'object') value = JSON.stringify(value);
        data.updated[key] = value || null;
        if (!data.updated[`updated_${key}`]) data.updated[`updated_${key}`] = null;
    } catch (error) {
        console.log(error);
    }
}

// ------------------------
//  Pricing-parquet sync path
// ------------------------

function formatPricingValue(val) {
    if (val === null || val === undefined) return null;
    if (typeof val === 'boolean') return val;
    const num = Number(val);
    if (!isNaN(num)) return Number(num.toFixed(2));
    if (typeof val === 'string') return val.trim();
    return val;
}

// Syncs one match's comp_* attributes from a pre-fetched pricing-parquet row
// map (keyed by base_sku|comp_source_store|comp_sku, see the job's batch
// loop). Same return shape as syncRecords.
export async function syncPricingParquetRecords(data, keysToUpdate, companyKey, pricingRowsMap, pricingParquetKeyMap) {
    try {
        data.processed = {};
        data.updated = {};

        if (!data.comp_sku) {
            data.processed.modified = false;
            data.processed.status = 'Sku Missing';
            return data;
        }

        const matchData = await getMatchWithMatchId(data);
        if (!matchData) {
            data.processed.modified = false;
            data.processed.status = 'Aurora Data Missing';
            return data;
        }

        const lookupKey = `${(data.base_sku || '').toLowerCase()}|${(data.comp_source_store || '').toLowerCase()}|${(data.comp_sku || '').toLowerCase()}`;
        const pricingRow = pricingRowsMap ? pricingRowsMap.get(lookupKey) : null;
        if (!pricingRow) {
            data.processed.modified = false;
            data.processed.status = 'Pricing Parquet Data Missing';
            return data;
        }

        const dqIssue = validatePricingParquetData(matchData, pricingRow, keysToUpdate, pricingParquetKeyMap, data.company_code, data.comp_source_store);
        if (dqIssue === 'missing') {
            data.processed.modified = false;
            data.processed.status = 'Pricing Parquet Data Missing';
            return data;
        } else if (dqIssue === 'error') {
            data.processed.modified = false;
            data.processed.isError = true;
            data.processed.status = 'Error in Processing Pricing Parquet Data';
            return data;
        } else if (dqIssue === 'none') {
            data.processed.modified = false;
            data.processed.status = 'No Changes Detected';
            return data;
        }

        const modifyValues = {};
        const auditData = { prev_value: {}, new_value: {} };

        for (const key of keysToUpdate) {
            if (key === 'comp_custom_attributes') {
                if (checkCompCustomAttributesFromPricing(pricingRow, matchData.comp_custom_attributes)) {
                    auditData.prev_value.comp_custom_attributes = matchData.comp_custom_attributes;
                    fillCompCustomAttributesFromPricing(modifyValues, pricingRow, matchData.comp_custom_attributes);
                    auditData.new_value.comp_custom_attributes = modifyValues.comp_custom_attributes;
                }
                continue;
            }

            const pricingKey = pricingParquetKeyMap[key];
            if (!pricingKey) {
                data.processed.modified = false;
                data.processed.status = 'Invalid Pricing Parquet Key';
                return data;
            }

            let newVal = pricingRow[pricingKey.val] ?? null;
            const oldVal = matchData[key] ?? null;

            if (key === 'comp_mfr_part_number' && isSentinelValue(newVal)) {
                newVal = pricingRow.model_number ?? null;
            }
            if (key === 'comp_pack_size' && typeof newVal === 'string') {
                newVal = newVal.split(' ')[0] || '';
            }
            if (key === 'comp_size' && data.company_code === 'ctc' && data.comp_source_store?.includes('amazonca')) {
                const sizeFromExtra = getAmazoncaValueFromPricingRow(pricingRow, 'size');
                if (sizeFromExtra !== null) newVal = sizeFromExtra;
            }
            if (key === 'comp_uom' && data.company_code === 'ctc' && data.comp_source_store?.includes('amazonca')) {
                const uomFromExtra = getAmazoncaValueFromPricingRow(pricingRow, 'uom');
                if (uomFromExtra !== null) newVal = uomFromExtra;
            }

            const formattedNew = formatPricingValue(newVal);
            const formattedOld = formatPricingValue(oldVal);
            if (formattedNew !== formattedOld) {
                modifyValues[key] = pricingKey.type === 'string' ? `${newVal}` : formattedNew;
                auditData.prev_value[key] = oldVal;
                auditData.new_value[key] = modifyValues[key];
            }
        }

        if (keysToUpdate.includes('comp_brand')) {
            const compBrandValue = modifyValues.comp_brand || matchData.comp_brand;
            if (compBrandValue && !isSentinelValue(compBrandValue) && matchData.comp_source_store) {
                auditData.prev_value.comp_brandtype = matchData.comp_brandtype;
                const brandType = await aurora.getCompBrandType(matchData.comp_source_store.toLowerCase(), compBrandValue?.toLowerCase());
                modifyValues.comp_brandtype = brandType || 'NB';
                auditData.new_value.comp_brandtype = modifyValues.comp_brandtype;
            }
            if (companyKey.copy_brand_type && !modifyValues.comp_brandtype && isSentinelValue(modifyValues.comp_brand)) {
                modifyValues.comp_brandtype = matchData.base_brandtype;
                auditData.new_value.comp_brandtype = modifyValues.comp_brandtype;
            }
        }

        if (keysToUpdate.includes('comp_pack_size')) {
            const compPackSize = modifyValues.comp_pack_size || matchData.comp_pack_size || 1;
            const compSize = modifyValues.comp_size || matchData.comp_size || 1;
            const normalizedCompSize = matchData.normalized_comp_size || 1;
            if (compPackSize && !isSentinelValue(compPackSize)) {
                auditData.prev_value.comp_total_size = matchData.comp_total_size;
                auditData.prev_value.normalized_comp_total_size = matchData.normalized_comp_total_size;

                const totalSize = (compPackSize * compSize) || 1;
                modifyValues.comp_total_size = totalSize.toString();
                const normalizedCompTotalSize = (compPackSize * normalizedCompSize) || 1;
                modifyValues.normalized_comp_total_size = normalizedCompTotalSize.toString();

                auditData.new_value.comp_total_size = totalSize.toString();
                auditData.new_value.normalized_comp_total_size = normalizedCompTotalSize.toString();
            }
        }

        if (matchData.active) {
            modifyValues.updated_manager_verified_date = new Date().toISOString();
        }

        if (Object.keys(modifyValues).length === 0) {
            data.processed.modified = false;
            data.processed.status = 'No Changes Detected';
            return data;
        }

        const resp = await aurora.updateMatchAttributes(data.company_code, data.base_source_store, data.comp_source_store, matchData.match_id, modifyValues);
        if (resp) {
            data.processed.response = resp;
            data.processed.modified = true;
            data.processed.status = 'Successfully Updated to DB';
        }

        for (const [k, v] of Object.entries(modifyValues)) {
            data.updated[`updated_${k}`] = v;
        }
        for (const key of keysToUpdate) {
            addToUpdatedField(matchData, data, key);
            if (key === 'comp_brand') addToUpdatedField(matchData, data, 'comp_brandtype');
        }

        data.auditData = auditData;
        return data;
    } catch (error) {
        console.log(`syncPricingParquetRecords error for match_id=${data?.match_id}: ${error.message}`);
        data.processed = { modified: false, isError: true, status: `Error: ${error.message}` };
        data.updated = data.updated || {};
        return data;
    }
}

function validatePricingParquetData(auroraData, pricingRow, keysToUpdate, keyMap, companyCode, compSourceStore) {
    try {
        if (!pricingRow) return 'missing';
        if (!auroraData) return 'auroraMissing';

        for (const auroraKey of keysToUpdate) {
            if (auroraKey === 'comp_custom_attributes') {
                if (checkCompCustomAttributesFromPricing(pricingRow, auroraData.comp_custom_attributes)) return 'detected';
                continue;
            }

            if ((auroraKey === 'comp_size' || auroraKey === 'comp_uom') && companyCode === 'ctc' && compSourceStore?.includes('amazonca')) {
                const extraVal = auroraKey === 'comp_size'
                    ? getAmazoncaValueFromPricingRow(pricingRow, 'size')
                    : getAmazoncaValueFromPricingRow(pricingRow, 'uom');
                if (extraVal !== null) {
                    const auroraVal = `${(auroraData[auroraKey] || '')}`.toLowerCase();
                    const lowerExtra = extraVal.toLowerCase();
                    if (lowerExtra !== '' && lowerExtra !== '-' && auroraVal !== lowerExtra) return 'detected';
                    continue;
                }
                // fall through to the standard pricing-parquet key check
            }

            const pricingKey = keyMap[auroraKey];
            if (!pricingKey) return 'error';
            const newVal = pricingRow[pricingKey.val];
            const oldVal = auroraData[auroraKey];
            const formattedNew = formatPricingValue(newVal);
            const formattedOld = formatPricingValue(oldVal);
            if ((oldVal == null && newVal != null) || formattedNew !== formattedOld) return 'detected';
        }
        return 'none';
    } catch (error) {
        console.log(error);
        return 'error';
    }
}

function checkCompCustomAttributesFromPricing(pricingRow, compCustomAttributes) {
    try {
        for (const [key, def] of Object.entries(PRICING_PARQUET_CUSTOM_ATTRIBUTE_KEYS)) {
            const auroraVal = compCustomAttributes && compCustomAttributes[key];
            const pricingVal = pricingRow && pricingRow[def.val];
            if (pricingVal && !isSentinelValue(pricingVal) && pricingVal != auroraVal) return true;
        }
        return false;
    } catch (error) {
        console.log('checkCompCustomAttributesFromPricing error', error);
        return false;
    }
}

function fillCompCustomAttributesFromPricing(modifyValues, pricingRow, existingCustomAttributes = {}) {
    const newCustomAttribute = { ...(existingCustomAttributes || {}) };
    for (const [key, def] of Object.entries(PRICING_PARQUET_CUSTOM_ATTRIBUTE_KEYS)) {
        const pricingVal = pricingRow && pricingRow[def.val];
        newCustomAttribute[key] = isSentinelValue(pricingVal) ? (existingCustomAttributes?.[key] ?? null) : `${pricingVal}`;
    }
    modifyValues.comp_custom_attributes = newCustomAttribute;
}
