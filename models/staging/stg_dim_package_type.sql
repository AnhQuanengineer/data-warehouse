WITH dim_package_type__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)
, dim_package_type__rename_column AS(
  SELECT
    package_type_id AS package_type_key
    ,package_type_name	AS package_type_name
  FROM dim_package_type__source
)
, dim_package_type__cast_type AS (
  SELECT
    CAST(package_type_key AS INTEGER) as package_type_key
    ,CAST(package_type_name AS STRING) as	package_type_name
  FROM dim_package_type__rename_column
)
, dim_package_type__add_undefined_record AS (
  SELECT
    package_type_key
    ,	package_type_name
  FROM dim_package_type__cast_type

  UNION ALL
  SELECT
    0 AS package_type_key
    ,'Undefined' AS package_type_name
  
  UNION ALL
  SELECT
    -1 AS package_type_key
    ,'Invalid' AS package_type_name
)
SELECT 
  COALESCE(package_type_key,0) AS package_type_key
  ,package_type_name
FROM dim_package_type__add_undefined_record AS dim_package_type
