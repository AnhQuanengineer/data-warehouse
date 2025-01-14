WITH fact_purchase_order_line__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
  
)
, fact_purchase_order_line__rename_column AS(
  SELECT
    purchase_order_line_id AS purchase_order_line_key
    ,description AS description
    ,purchase_order_id  AS purchase_order_key
    ,stock_item_id AS product_key
    ,package_type_id AS package_type_key
    ,last_receipt_date
    ,ordered_outers
    ,received_outers 
    ,expected_unit_price_per_outer
    ,is_order_line_finalized AS is_order_line_finalized_boolean
  FROM fact_purchase_order_line__source
)
, fact_purchase_order_line__cast_type AS (
  SELECT
    CAST(purchase_order_line_key AS INTEGER) as purchase_order_line_key
    ,CAST(description AS STRING) as description
    ,CAST(purchase_order_key AS INTEGER) as purchase_order_key
    ,CAST(product_key AS INTEGER) as	product_key
    ,CAST(package_type_key AS INTEGER) as	package_type_key
    ,CAST(last_receipt_date AS DATE) as last_receipt_date
    ,CAST(ordered_outers AS INTEGER) as ordered_outers
    ,CAST(received_outers AS INTEGER) as received_outers
    ,CAST(expected_unit_price_per_outer AS NUMERIC) as expected_unit_price_per_outer
    ,CAST(is_order_line_finalized_boolean AS BOOLEAN) as is_order_line_finalized_boolean
  FROM fact_purchase_order_line__rename_column
),fact_purchase_order_line__convert_bollean AS (
  SELECT
    purchase_order_line_key
    ,description
    ,purchase_order_key
    ,product_key
    ,is_order_line_finalized_boolean
    ,CASE 
        WHEN is_order_line_finalized_boolean IS TRUE THEN 'Order Finalized Line'
        WHEN is_order_line_finalized_boolean IS FALSE THEN 'Not Order Finalized Line'
        WHEN is_order_line_finalized_boolean IS NULL THEN 'Undefined' 
        ELSE 'Invalid'
        END AS is_order_finalized_line
    ,package_type_key
    ,last_receipt_date
    ,ordered_outers
    ,received_outers
    ,expected_unit_price_per_outer
  FROM fact_purchase_order_line__cast_type
)
SELECT 
  fact_purchase_line.purchase_order_line_key
  ,fact_purchase_line.description 
  ,fact_purchase_line.purchase_order_key
  ,fact_purchase_line.product_key
  ,COALESCE(fact_purchase_header.contact_person_key,-1) AS contact_person_key
  ,COALESCE(fact_purchase_header.supplier_key,-1) AS supplier_key
  ,FARM_FINGERPRINT(CONCAT(
    COALESCE(fact_purchase_header.is_order_finalized,'Undefined')
    , ','
    ,COALESCE(fact_purchase_line.is_order_finalized_line,'Undefined')
    , ','
    ,fact_purchase_line.package_type_key
    , ','
    ,fact_purchase_header.delivery_method_key
  )) AS purchase_order_line_indicator_key
  ,fact_purchase_header.order_date
  ,fact_purchase_line.last_receipt_date
  ,fact_purchase_line.ordered_outers
  ,fact_purchase_line.received_outers
  ,fact_purchase_line.expected_unit_price_per_outer
  ,fact_purchase_line.received_outers*fact_purchase_line.expected_unit_price_per_outer AS expected_unit_price_outer
FROM fact_purchase_order_line__convert_bollean as fact_purchase_line 
LEFT JOIN {{ ref ('stg_fact_purchase_order') }} as fact_purchase_header
ON fact_purchase_header.purchase_order_key = fact_purchase_line.purchase_order_key
