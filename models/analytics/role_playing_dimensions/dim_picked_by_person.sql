SELECT
  person_key AS picked_by_person_key
  ,full_name AS picked_by_person_full_name
FROM {{ ref('dim_person') }}