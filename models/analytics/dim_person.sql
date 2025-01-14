
WITH dim_person__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__people`
)
, dim_person__rename_column AS(
  SELECT 
    person_id	AS person_key
    ,full_name	AS full_name
  FROM dim_person__source
)
, dim_person__cast_type AS(
  SELECT
    CAST(person_key AS integer) as	person_key
    ,CAST(full_name AS string) as	full_name
  FROM dim_person__rename_column
)
,dim_person__add_undefined_record AS(
  SELECT
    person_key
    ,full_name
  FROM dim_person__cast_type

  UNION ALL
  SELECT
    0 AS person_key
    ,'Undefined'  AS full_name

  UNION ALL
  SELECT
    -1 AS person_key
    ,'Error'  AS full_name
)
SELECT 
  person_key
  ,full_name
FROM dim_person__add_undefined_record

