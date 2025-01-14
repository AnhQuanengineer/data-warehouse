WITH dim_customer__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
) 
, dim_customer__rename_column AS (
  SELECT 
    customer_id	AS customer_key
    ,customer_name	AS customer_name
    ,buying_group_id AS  buying_group_key
    ,customer_category_id AS customer_category_key
    ,is_on_credit_hold AS is_on_credit_hold_boolean
    ,is_statement_sent AS is_statement_sent_boolean
    ,phone_number
    ,credit_limit
    ,payment_days
    ,standard_discount_percentage
    ,account_opened_date 
    ,delivery_method_id AS delivery_method_key
    ,bill_to_customer_id AS bill_to_customer_key
    ,delivery_city_id AS delivery_city_key
    ,postal_city_id AS postal_city_key
    ,primary_contact_person_id AS primary_contact_person_key
    ,alternate_contact_person_id AS alternate_contact_person_key
  FROM dim_customer__source
)
, dim_customer__cast_type AS(
  SELECT
    CAST(customer_key AS INTEGER) AS customer_key
    ,CAST(customer_name AS STRING) AS customer_name
    ,CAST(buying_group_key AS INTEGER) AS buying_group_key
    ,CAST(customer_category_key AS INTEGER) AS customer_category_key
    ,CAST(is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
    ,CAST(is_statement_sent_boolean AS BOOLEAN) AS is_statement_sent_boolean
    ,CAST(phone_number AS STRING) AS phone_number
    ,CAST(credit_limit AS NUMERIC) AS credit_limit
    ,CAST(payment_days AS INTEGER) AS payment_days
    ,CAST(standard_discount_percentage AS NUMERIC) AS standard_discount_percentage
    ,CAST(account_opened_date AS TIMESTAMP) AS account_opened_date
    ,CAST(delivery_method_key AS INTEGER) AS delivery_method_key
    ,CAST(bill_to_customer_key AS INTEGER) AS bill_to_customer_key
    ,CAST(delivery_city_key AS INTEGER) AS delivery_city_key
    ,CAST(postal_city_key AS INTEGER) AS postal_city_key
    ,CAST(primary_contact_person_key AS INTEGER) AS primary_contact_person_key
    ,CAST(alternate_contact_person_key AS INTEGER) AS alternate_contact_person_key
  FROM dim_customer__rename_column
)
,dim_customer__convert_boolean AS(
  SELECT
    customer_key
    ,customer_name
    ,buying_group_key
    ,customer_category_key
    ,CASE 
      WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
      WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
      WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
      END AS is_on_credit_hold
    ,CASE 
      WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
      WHEN is_statement_sent_boolean IS FALSE THEN 'Not Statement Sent'
      WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
      END AS is_statement_sent
    ,phone_number
    ,credit_limit
    ,payment_days
    ,standard_discount_percentage
    ,account_opened_date
    ,delivery_method_key
    ,bill_to_customer_key
    ,delivery_city_key
    ,postal_city_key
    ,primary_contact_person_key
    ,alternate_contact_person_key
  FROM dim_customer__cast_type
)
,dim_customer__add_undefined_record AS(
  SELECT
    customer_key
    ,customer_name
    ,is_on_credit_hold
    ,is_statement_sent
    ,customer_category_key
    ,buying_group_key
    ,phone_number
    ,credit_limit
    ,payment_days
    ,standard_discount_percentage
    ,delivery_method_key
    ,bill_to_customer_key
    ,delivery_city_key
    ,postal_city_key
    ,primary_contact_person_key
    ,alternate_contact_person_key
  FROM dim_customer__convert_boolean

  UNION ALL 
  SELECT
    0 AS customer_key
    ,'Undefined' AS customer_name
    ,'Undefined' AS is_on_credit_hold
    ,'Undefined' AS is_statement_sent
    , 0 AS customer_category_key
    , 0 AS buying_group_key
    ,'Undefined' AS phone_number
    , 0 AS credit_limit
    , 0 as payment_days
    , 0 AS standard_discount_percentage
    , 0 AS delivery_method_key  
    , 0 AS bill_to_customer_key
    , 0 AS delivery_city_key
    , 0 AS postal_city_key
    , 0 AS primary_contact_person_key
    , 0 AS alternate_contact_person_key
  UNION ALL 
  SELECT
    -1 AS customer_key
    ,'Invalid' AS customer_name
    ,'Invalid' AS is_on_credit_hold
    ,'Invalid' AS is_statement_sent
    , -1 AS customer_category_key
    , -1 AS buying_group_key
    ,'Invalid' AS phone_number
    , -1 AS credit_limit
    , -1 AS payment_days
    , -1 AS standard_discount_percentage
    , -1 AS delivery_method_key
    , -1 AS bill_to_customer_key
    , -1 AS delivery_city_key
    , -1 AS postal_city_key
    , -1 AS primary_contact_person_key
    , -1 AS alternate_contact_person_key
)
SELECT 
  dim_customer.customer_key 
  ,dim_customer.customer_name
  ,dim_customer.is_on_credit_hold
  ,dim_customer.is_statement_sent
  ,COALESCE(dim_customer.buying_group_key,-1) AS buying_group_key
  ,COALESCE(dim_buying_group.buying_group_name,'Invalid') AS buying_group_name
  ,dim_customer.customer_category_key
  ,COALESCE(dim_customer_category.customer_category_name,'Invalid') AS customer_category_name
  ,COALESCE(dim_customer.phone_number,'Invalid') AS phone_number
  ,COALESCE(dim_customer.credit_limit,0) AS credit_limit
  ,dim_customer.payment_days
  ,dim_customer.standard_discount_percentage
  ,COALESCE(dim_customer.delivery_method_key,-1) AS delivery_method_key
  ,dim_delivery_method.delivery_method_name
  ,COALESCE(dim_customer.bill_to_customer_key,0) AS bill_to_customer_key
  ,COALESCE(dim_customer.delivery_city_key,-1) AS delivery_city_key
  ,dim_delivery_city.city_name AS delivery_city_name
  ,COALESCE(dim_customer.postal_city_key,-1) AS postal_city_key
  ,dim_postal_city.city_name AS postal_city_name
  ,COALESCE(dim_customer.primary_contact_person_key,-1) AS primary_contact_person_key
  ,dim_primary_contact_person.full_name AS primary_contact_person_full_name
  ,COALESCE(dim_customer.alternate_contact_person_key,-1) AS alternate_contact_person_key
  ,COALESCE(dim_alternate_contact_person.full_name,'Error') AS alternate_contact_person_full_name
FROM dim_customer__add_undefined_record as dim_customer
LEFT JOIN {{ ref('stg_dim_customer_category') }} AS dim_customer_category
  ON dim_customer_category.customer_category_key = dim_customer.customer_category_key
LEFT JOIN {{ ref('stg_dim_buying_group') }} AS dim_buying_group
  ON dim_buying_group.buying_group_key = dim_customer.buying_group_key
LEFT JOIN {{ ref('stg_dim_delivery_method') }} AS dim_delivery_method
  ON dim_delivery_method.delivery_method_key = dim_customer.delivery_method_key
LEFT JOIN {{ ref('stg_dim_city') }} AS dim_delivery_city
  ON dim_delivery_city.city_key = dim_customer.delivery_city_key
LEFT JOIN {{ ref('stg_dim_city') }} AS dim_postal_city
  ON dim_postal_city.city_key = dim_customer.postal_city_key
LEFT JOIN {{ ref('dim_person') }} AS dim_primary_contact_person
  ON dim_primary_contact_person.person_key = dim_customer.primary_contact_person_key
LEFT JOIN {{ ref('dim_person') }} AS dim_alternate_contact_person
  ON dim_alternate_contact_person.person_key = dim_customer.alternate_contact_person_key