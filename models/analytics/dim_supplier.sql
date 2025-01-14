WITH dim_supplier__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)
, dim_supplier__rename_column AS(
  SELECT 
    supplier_id AS supplier_key
    ,supplier_name AS	supplier_name
    ,bank_account_name
    ,bank_account_number
    ,payment_days
    ,supplier_category_id as supplier_category_key
    ,delivery_method_id AS delivery_method_key
  FROM dim_supplier__source
)
, dim_supplier__cast_type AS(
  SELECT
    CAST(supplier_key AS integer) as	supplier_key
    ,CAST(supplier_name AS string) as	supplier_name
    ,CAST(bank_account_name AS string) as	bank_account_name
    ,CAST(bank_account_number AS string) as	bank_account_number
    ,CAST(payment_days AS integer) as	payment_day
    ,CAST(supplier_category_key AS integer) as	supplier_category_key
    ,CAST(delivery_method_key AS integer) as	delivery_method_key
  FROM dim_supplier__rename_column
)
SELECT 
  supplier.supplier_key
  ,supplier.supplier_name
  ,supplier.bank_account_name
  ,supplier.bank_account_number
  ,supplier.payment_day
  ,supplier.supplier_category_key
  ,supplier_category.supplier_category_name
  ,COALESCE(supplier.delivery_method_key,-1) AS delivery_method_key
  ,COALESCE(delivery_method.delivery_method_name,'Invalid') AS delivery_method_name
FROM dim_supplier__cast_type as supplier
LEFT JOIN {{ ref ('stg_dim_supplier_category') }} as supplier_category
ON supplier_category.supplier_category_key = supplier.supplier_category_key
LEFT JOIN {{ ref ('stg_dim_delivery_method') }} as delivery_method
ON delivery_method.delivery_method_key = supplier.delivery_method_key


