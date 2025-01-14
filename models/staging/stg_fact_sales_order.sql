WITH fact_sales_order__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)
, fact_sales_order__rename_column AS(
  SELECT
    order_id AS sales_order_key
    ,customer_id	AS customer_key
    ,picked_by_person_id	AS picked_by_person_key
    ,order_date AS order_date
    ,salesperson_person_id AS salesperson_person_key
    ,contact_person_id AS contact_person_key
    ,is_undersupply_backordered AS is_undersupply_backordered_bollean
    ,expected_delivery_date
    ,picking_completed_when
  FROM fact_sales_order__source
)
, fact_sales_order__cast_type AS (
  SELECT
    CAST(sales_order_key AS INTEGER) as sales_order_key
    ,CAST(customer_key AS INTEGER) as	customer_key
    ,CAST(picked_by_person_key AS INTEGER) as	picked_by_person_key
    ,CAST(salesperson_person_key AS INTEGER) as	salesperson_person_key
    ,CAST(contact_person_key AS INTEGER) as	contact_person_key
    ,CAST(is_undersupply_backordered_bollean AS BOOLEAN) as	is_undersupply_backordered_bollean
    ,CAST(order_date AS DATE) as	order_date
    ,CAST(expected_delivery_date AS DATE) as expected_delivery_date
    ,CAST(picking_completed_when AS DATE) as	picking_completed_when
  FROM fact_sales_order__rename_column
)
,fact_sales_order__convert_bollean AS (
  SELECT
    sales_order_key
    ,customer_key
    ,picked_by_person_key
    ,salesperson_person_key
    ,contact_person_key
    ,is_undersupply_backordered_bollean
    ,CASE 
        WHEN is_undersupply_backordered_bollean IS TRUE THEN 'Undersupply Backordered'
        WHEN is_undersupply_backordered_bollean IS FALSE THEN 'Not Undersupply Backordered'
        WHEN is_undersupply_backordered_bollean IS NULL THEN 'Undefined' 
        ELSE 'Invalid'
        END AS is_undersupply_backordered
    ,order_date
    ,expected_delivery_date
    ,picking_completed_when
  FROM fact_sales_order__cast_type
)
SELECT 
  fact_sales_order.sales_order_key
  ,COALESCE(fact_sales_order.customer_key,-1) AS customer_key
  ,dim_customer.customer_name AS customer_name
  ,COALESCE(fact_sales_order.picked_by_person_key,0) AS picked_by_person_key
  ,COALESCE(dim_picked_by_person_key.full_name,'Undefined') AS picked_by_person_name
  ,COALESCE(fact_sales_order.salesperson_person_key,0) AS salesperson_person_key
  ,dim_salesperson_person.full_name AS salesperson_person_name
  ,COALESCE(fact_sales_order.contact_person_key,0) AS contact_person_key
  ,dim_contact_person.full_name AS contact_person_name
  ,fact_sales_order.is_undersupply_backordered
  ,fact_sales_order.order_date
  ,fact_sales_order.expected_delivery_date
  ,fact_sales_order.picking_completed_when
FROM fact_sales_order__convert_bollean AS fact_sales_order
LEFT JOIN {{ ref('dim_person') }} AS dim_salesperson_person
  ON dim_salesperson_person.person_key = fact_sales_order.salesperson_person_key
LEFT JOIN {{ ref('dim_person') }} AS dim_contact_person
  ON dim_contact_person.person_key = fact_sales_order.contact_person_key
LEFT JOIN {{ ref('dim_person') }} AS dim_picked_by_person_key
  ON dim_picked_by_person_key.person_key = fact_sales_order.picked_by_person_key
LEFT JOIN {{ ref('dim_customer') }} AS dim_customer
  ON dim_customer.customer_key = fact_sales_order.customer_key
