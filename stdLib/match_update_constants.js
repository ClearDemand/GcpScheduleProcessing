// ----------------------------------------------------------------------------
//  Constants for the Match Update Processor (ES module).
//  Ported from PmtScheduleProcessing/poll.js, which had 3-4 inconsistent
//  copies of the "treat this string as no value" list (some missing 'na',
//  some missing ' ' vs '', etc.) — consolidated here into one canonical set
//  used everywhere instead.
// ----------------------------------------------------------------------------

export const SENTINEL_VALUES = new Set([
    '-', 'n/a', 'na', 'nan', 'none', 'undefined', 'null', '_', '0', '', ' '
]);

// True for null/undefined or any string that means "no value" (case/whitespace
// insensitive) per the sentinel list above.
export function isSentinelValue(value) {
    if (value === null || value === undefined) return true;
    return SENTINEL_VALUES.has(`${value}`.trim().toLowerCase());
}

// comp_* match column -> GPC (DynamoDB) attribute name, ported from poll.js's
// dqKeys.
export const GPC_KEY_MAP = {
    comp_sku: { val: 'sku', type: 'string' },
    comp_upc: { val: 'upc', type: 'string' },
    comp_custom_sku: { val: 'custom_sku', type: 'string' },
    comp_parent_sku: { val: 'parent_sku', type: 'string' },
    comp_title: { val: 'product_title', type: 'string' },
    comp_size: { val: 'size', type: 'string' },
    comp_uom: { val: 'uom', type: 'string' },
    comp_mfr_part_number: { val: 'model_number', type: 'string' },
    comp_url: { val: 'product_url', type: 'string' },
    comp_img: { val: 'image_url', type: 'string' },
    comp_brand: { val: 'brand', type: 'string' },
    comp_category: { val: 'category', type: 'string' },
    comp_subcategory: { val: 'subcategory', type: 'string' },
    comp_sub_subcategory: { val: 'sub_subcategory', type: 'string' },
    comp_price: { val: 'list_price', type: 'string' },
    comp_alt_price: { val: 'reg_price_alt', type: 'string' },
    comp_alt_size: { val: 'size_alt', type: 'string' },
    comp_alt_uom: { val: 'uom_alt', type: 'string' },
    comp_shipping_weight: { val: 'shipping_weight', type: 'string' },
    comp_dimensions: { val: 'dimensions', type: 'string' },
    comp_description: { val: 'product_description', type: 'string' },
    comp_pack_size: { val: 'pack_size', type: 'string' }
};

// comp_* match column -> pricing-parquet column name, ported from poll.js's
// pricingParquetKeyMap.
export const PRICING_PARQUET_KEY_MAP = {
    comp_sku: { val: 'sku', type: 'string' },
    comp_upc: { val: 'upc', type: 'string' },
    comp_custom_sku: { val: 'comp_custom_sku', type: 'string' },
    comp_parent_sku: { val: 'parent_sku', type: 'string' },
    comp_title: { val: 'product_title', type: 'string' },
    comp_size: { val: 'size', type: 'string' },
    comp_uom: { val: 'uom', type: 'string' },
    comp_mfr_part_number: { val: 'model_number', type: 'string' },
    comp_url: { val: 'product_url', type: 'string' },
    comp_img: { val: 'image_url', type: 'string' },
    comp_brand: { val: 'brand', type: 'string' },
    comp_category: { val: 'category', type: 'string' },
    comp_subcategory: { val: 'subcategory', type: 'string' },
    comp_sub_subcategory: { val: 'sub_subcategory', type: 'string' },
    comp_price: { val: 'effective_price', type: 'string' },
    comp_alt_price: { val: 'list_price_alt', type: 'string' },
    comp_alt_size: { val: 'size_alt', type: 'string' },
    comp_alt_uom: { val: 'uom_alt', type: 'string' },
    comp_shipping_weight: { val: 'shipping_weight', type: 'string' },
    comp_dimensions: { val: 'dimension', type: 'string' },
    comp_description: { val: 'product_description', type: 'string' },
    comp_pack_size: { val: 'pack_size', type: 'string' }
};

// comp_custom_attributes sub-keys sourced from GPC, ported from poll.js's
// customAttributesKeys. level 1 = top-level GPC field, level 2 = nested under
// gpcData.pharmacy.
export const GPC_CUSTOM_ATTRIBUTE_KEYS = {
    comp_product_total_uom: { val: 'product_total_uom', type: 'string', level: '1' },
    comp_product_total_size: { val: 'product_total_size', type: 'string', level: '1' },
    comp_strength_concentration: { val: 'strength_concentration', type: 'string', level: '2' },
    comp_strength_concentration_uom: { val: 'strength_concentration_uom', type: 'string', level: '2' },
    comp_pharmacy_package_quantity: { val: 'pharmacy_package_quantity', type: 'string', level: '2' },
    comp_pharmacy_package_quantity_uom: { val: 'pharmacy_package_quantity_uom', type: 'string', level: '2' },
    comp_total_quantity: { val: 'package_quantity', type: 'string', level: '2' },
    comp_total_quantity_uom: { val: 'package_quantity_uom', type: 'string', level: '2' }
};

// comp_custom_attributes sub-keys sourced from the pricing-parquet dataset,
// ported from poll.js's pricingParquetCustomAttributesKeys.
export const PRICING_PARQUET_CUSTOM_ATTRIBUTE_KEYS = {
    comp_product_total_uom: { val: 'pricing_comp_product_total_uom', type: 'string', level: '1' },
    comp_product_total_size: { val: 'pricing_comp_product_total_size', type: 'string', level: '1' },
    comp_strength_concentration: { val: 'pricing_comp_strength_concentration_size', type: 'string', level: '2' },
    comp_strength_concentration_uom: { val: 'pricing_comp_strength_concentration_uom', type: 'string', level: '2' }
};
