WITH dim_delivery_method__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)
, dim_delivery_method__rename_column AS(
  SELECT
    delivery_method_id AS delivery_method_key
    , delivery_method_name	AS  delivery_method_name
  FROM dim_delivery_method__source
)
, dim_delivery_method__cast_type AS (
  SELECT
    CAST( delivery_method_key AS INTEGER) as delivery_method_key
    ,CAST( delivery_method_name AS STRING) as	delivery_method_name
  FROM dim_delivery_method__rename_column
)
, dim_delivery_method__add_undefined_record AS (
  SELECT
    delivery_method_key
    ,	delivery_method_name
  FROM dim_delivery_method__cast_type

  UNION ALL
  SELECT
    0 AS delivery_method_key
    ,'Undefined' AS delivery_method_name
  
  UNION ALL
  SELECT
    -1 AS delivery_method_key
    ,'Invalid' AS delivery_method_name
)
SELECT 
  COALESCE(delivery_method_key,0) AS delivery_method_key
  ,delivery_method_name
FROM dim_delivery_method__add_undefined_record  AS dim_delivery_method