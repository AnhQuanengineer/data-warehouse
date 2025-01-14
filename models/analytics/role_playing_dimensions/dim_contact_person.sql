SELECT
  person_key AS contact_person_key
  ,full_name AS contact_person_full_name
FROM {{ ref('dim_person') }}