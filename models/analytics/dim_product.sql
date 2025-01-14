WITH dim_product__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)
, dim_product__rename_column AS(
  SELECT 
    stock_item_id AS product_key
    ,stock_item_name AS product_name
    ,brand AS brand_name
    ,supplier_id AS supplier_key
    ,is_chiller_stock AS is_chiller_stock_boolean
    ,unit_price
    ,recommended_retail_price
    ,barcode AS bar_code
    ,lead_time_days
    ,unit_package_id as unit_package_type_key
    ,outer_package_id AS outer_package_type_key
    ,color_id AS color_key
  FROM dim_product__source
)
, dim_product__cast_type AS(
  SELECT
    CAST(product_key AS integer) as	product_key
    ,CAST(product_name AS string) as	product_name
    ,CAST(brand_name AS string) as	brand_name
    ,CAST(supplier_key AS integer) as	supplier_key
    ,CAST(is_chiller_stock_boolean AS BOOLEAN) AS is_chiller_stock_boolean
    ,CAST(unit_price AS NUMERIC) as	unit_price
    ,CAST(recommended_retail_price AS NUMERIC) as	recommended_retail_price
    ,CAST(bar_code AS string) as	bar_code
    ,CAST(lead_time_days AS integer) as	lead_time_days
    ,CAST(unit_package_type_key AS integer) as	unit_package_type_key
    ,CAST(outer_package_type_key AS integer) as	outer_package_type_key
    ,CAST(color_key AS integer) as	color_key
  FROM dim_product__rename_column
)
,dim_product__convert_bollean AS(
  SELECT
    product_key
    ,product_name
    ,brand_name
    ,supplier_key
    ,CASE 
      WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
      WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
      END AS is_chiller_stock
    ,unit_price
    ,recommended_retail_price
    ,bar_code
    ,lead_time_days
    ,unit_package_type_key
    ,outer_package_type_key
    ,color_key
  FROM dim_product__cast_type
)
,dim_product__handdle_null AS(
  SELECT
    product_key
    ,product_name
    ,COALESCE(brand_name,'Undefined') AS brand_name
    ,supplier_key
    ,is_chiller_stock
    ,unit_price
    ,recommended_retail_price
    ,COALESCE(bar_code,'Undefined') AS bar_code
    ,lead_time_days
    ,unit_package_type_key
    ,outer_package_type_key
    ,color_key
  FROM dim_product__convert_bollean
)
SELECT 
  dim_product.product_key
  ,dim_product.product_name
  ,dim_product.brand_name
  ,dim_product.is_chiller_stock
  ,dim_product.supplier_key
  ,COALESCE(dim_supplier.supplier_name,'Invalid') AS supplier_name
  ,dim_product.unit_price
  ,dim_product.recommended_retail_price
  ,dim_product.bar_code
  ,dim_product.lead_time_days
  ,COALESCE(dim_product.unit_package_type_key,-1) AS unit_package_type_key
  ,dim_unit_package_type.package_type_name AS unit_package_type_name
  ,COALESCE(dim_product.outer_package_type_key,-1) AS outer_package_type_key
  ,dim_outer_package_type.package_type_name AS outer_package_type_name
  ,COALESCE(dim_product.color_key,-1) AS color_key
  ,COALESCE(dim_color.color_name,'Invalid') as color_name
FROM dim_product__handdle_null AS dim_product
LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key
LEFT JOIN {{ ref('stg_dim_package_type') }} AS dim_unit_package_type
  ON dim_product.unit_package_type_key = dim_unit_package_type.package_type_key
LEFT JOIN {{ ref('stg_dim_package_type') }} AS dim_outer_package_type
  ON dim_product.outer_package_type_key = dim_outer_package_type.package_type_key
LEFT JOIN {{ ref('stg_dim_color') }} AS dim_color
  ON dim_product.color_key = dim_color.color_key
