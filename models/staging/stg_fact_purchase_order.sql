WITH fact_purchase_order__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)
, fact_purchase_order__rename_column AS(
  SELECT
    purchase_order_id AS purchase_order_key
    ,supplier_id	AS supplier_key
    ,delivery_method_id	AS delivery_method_key
    ,contact_person_id AS contact_person_key
    ,order_date AS order_date
    ,expected_delivery_date 
    ,supplier_reference 
    ,is_order_finalized AS is_order_finalized_boolean
  FROM fact_purchase_order__source
)
, fact_purchase_order__cast_type AS (
  SELECT
    CAST(purchase_order_key AS INTEGER) as purchase_order_key
    ,CAST(supplier_key AS INTEGER) as	supplier_key
    ,CAST(delivery_method_key AS INTEGER) as	delivery_method_key
    ,CAST(contact_person_key AS INTEGER) as	contact_person_key
    ,CAST(supplier_reference AS STRING) as	supplier_reference
    ,CAST(is_order_finalized_boolean AS BOOLEAN) as	is_order_finalized_boolean
    ,CAST(order_date AS DATE) as	order_date
    ,CAST(expected_delivery_date AS DATE) as expected_delivery_date
  FROM fact_purchase_order__rename_column
)
,fact_purchase_order__convert_bollean AS (
  SELECT
    purchase_order_key
    ,supplier_key
    ,delivery_method_key
    ,contact_person_key
    ,is_order_finalized_boolean
    ,CASE 
        WHEN is_order_finalized_boolean IS TRUE THEN 'Order Finalized'
        WHEN is_order_finalized_boolean IS FALSE THEN 'Not Order Finalized'
        WHEN is_order_finalized_boolean IS NULL THEN 'Undefined' 
        ELSE 'Invalid'
        END AS is_order_finalized
    ,order_date
    ,expected_delivery_date
    ,supplier_reference
  FROM fact_purchase_order__cast_type
)
SELECT 
  fact_purchase_order.purchase_order_key
  ,fact_purchase_order.supplier_key
  ,dim_supplier.supplier_name
  ,COALESCE(fact_purchase_order.delivery_method_key,-1) AS delivery_method_key
  ,dim_delivery_method.delivery_method_name
  ,COALESCE(fact_purchase_order.contact_person_key,-1) AS contact_person_key
  ,dim_contact_person.contact_person_full_name
  ,fact_purchase_order.is_order_finalized
  ,fact_purchase_order.order_date
  ,fact_purchase_order.expected_delivery_date
  ,COALESCE(fact_purchase_order.supplier_reference,'Undefined') AS supplier_reference
FROM fact_purchase_order__convert_bollean AS fact_purchase_order
LEFT JOIN {{ ref('dim_contact_person') }} AS dim_contact_person
  ON dim_contact_person.contact_person_key = fact_purchase_order.contact_person_key
LEFT JOIN {{ ref('stg_dim_delivery_method') }} AS dim_delivery_method
  ON dim_delivery_method.delivery_method_key = fact_purchase_order.delivery_method_key
LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_supplier.supplier_key = fact_purchase_order.supplier_key
