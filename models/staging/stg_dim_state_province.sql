WITH dim_state_province__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)
, dim_state_province__rename_column AS(
  SELECT
    state_province_id AS state_province_key
    ,state_province_name	AS state_province_name
  FROM dim_state_province__source
)
, dim_state_province__cast_type AS (
  SELECT
    CAST(state_province_key AS INTEGER) as state_province_key
    ,CAST(state_province_name AS STRING) as	state_province_name
  FROM dim_state_province__rename_column
)
, dim_state_province__add_undefined_record AS (
  SELECT
    state_province_key
    ,	state_province_name
  FROM dim_state_province__cast_type

  UNION ALL
  SELECT
    0 AS state_province_key
    ,'Undefined' AS state_province_name
  
  UNION ALL
  SELECT
    -1 AS state_province_key
    ,'Invalid' AS state_province_name
)
SELECT 
  COALESCE(state_province_key,0) AS state_province_key
  ,state_province_name
FROM dim_state_province__add_undefined_record AS dim_state_province
