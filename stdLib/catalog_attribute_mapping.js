// ----------------------------------------------------------------------------
//  Default catalog attribute mapping for the catalog-sync job.
//
//  Keys are target base_* columns; `sources` is a falsy-coalesce chain
//  evaluated in order. This default applies to every tenant. A tenant that
//  needs a different mapping sets `catalog_attribute_mapping_overrides` on its
//  company-code doc (companyCodeLibrary) — a partial object in the same shape,
//  merged column-by-column on top of this default (buildCatalogSyncPlan in
//  aurora_resources.js). Tenant-specific behaviour that isn't a full column
//  override is still expressed through `enabled_by_flag` entries here, which
//  read the same company-code doc.
//
//  Transform vocabulary the consumer (aurora_resources.js) implements:
//    sources                      falsy-coalesce in order
//    coalesce: non_empty_string   skip '' as well as falsy (the .length > 0 checks)
//    fallback                     applied when every source is falsy
//    prefix                       prepend literal `prefix` to `source`
//    json_extract                 pull `key` out of the JSON `source` column
//    first_token                  String(value).split(' ')[0]
//    variants                     first entry whose flag is enabled; a flagless
//                                 entry is the default
//    recompute_total_size         when this column syncs, also recompute
//                                 base_total_size = size * pack_size (no
//                                 standalone base_total_size mapping entry)
// ----------------------------------------------------------------------------
export const DEFAULT_CATALOG_ATTRIBUTE_MAPPING = {
    base_upc:              { sources: ['upc'] },
    base_parent_sku:       { sources: ['parent_sku'], default: null },
    base_sku:              { sources: ['sku'] },
    base_custom_sku:       { sources: ['custom_sku', 'sku'] },

    base_custom_attributes: {
        source: 'additional_attributes',
        enabled_by_flag: 'allowAdditionalAttribute',
        default: null
    },

    base_price:            { sources: ['effective_price', 'reg_price', 'list_price'] },
    base_alt_price:        { sources: ['list_price_alt'] },
    base_url:              { sources: ['product_url'] },
    base_img:              { sources: ['image_url'] },
    base_brand:            { sources: ['brand'] },
    base_brandtype:        { sources: ['brand_type'] },
    base_title:            { sources: ['product_title'] },
    base_description:      { sources: ['product_description'] },

    base_category:             { sources: ['category'] },
    base_subcategory:          { sources: ['subcategory'] },
    base_sub_subcategory:      { sources: ['sub_subcategory'] },
    base_sub_sub_subcategory:  { sources: ['sub_sub_subcategory'] },
    base_sub_sub_sub_subcategory: {
        source: 'additional_attributes',
        transform: 'json_extract',
        key: 'sub_sub_sub_subcategory',
        enabled_by_flag: 'json_category_override_enabled'
    },

    // recompute_total_size: base_total_size = size * pack_size is recomputed
    // whenever either of these two columns syncs (buildCatalogSyncPlan), for
    // every tenant whose catalog has both size and pack_size columns — no
    // standalone base_total_size mapping entry is needed.
    base_size:             { sources: ['size', 'product_weight_size'], default: null, recompute_total_size: true },
    base_uom:              { sources: ['uom', 'product_weight_uom'], default: null },
    base_shipping_weight:  { sources: ['shipping_weight'] },
    base_dimensions:       { sources: ['dimension', 'dimensions'] },

    base_match_sku:        { sources: ['match_sku'], default: null },

    base_mfr_part_number: {
        sources: ['manufacturer_part_number', 'model_number'],
        coalesce: 'non_empty_string',
        default: null
    },

    base_pack_size: {
        source: 'pack_size',
        transform: 'first_token',
        coalesce: 'non_empty_string',
        default: null,
        recompute_total_size: true
    }
};
