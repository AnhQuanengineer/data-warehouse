WITH fact_sales_order_line__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
  
)
, fact_sales_order_line__rename_column AS(
  SELECT
    order_line_id AS sales_order_line_key
    ,description AS description
    ,order_id  AS sales_order_key
    ,stock_item_id AS product_key
    ,package_type_id AS package_type_key
    ,quantity 
    ,unit_price 
    ,picking_completed_when AS line_picking_completed_when
    ,tax_rate 
  FROM fact_sales_order_line__source
)
, fact_sales_order_line__cast_type AS (
  SELECT
    CAST(sales_order_line_key AS INTEGER) as sales_order_line_key
    ,CAST(description AS STRING) as description
    ,CAST(sales_order_key AS INTEGER) as sales_order_key
    ,CAST(product_key AS INTEGER) as	product_key
    ,CAST(package_type_key AS INTEGER) as	package_type_key
    ,CAST(quantity AS INTEGER) as quantity
    ,CAST(unit_price AS NUMERIC) as unit_price
    ,CAST(line_picking_completed_when AS DATE) as line_picking_completed_when
    ,CAST(tax_rate AS NUMERIC) as tax_rate
  FROM fact_sales_order_line__rename_column
)
SELECT 
  fact_line.sales_order_line_key
  ,fact_line.description AS description
  ,fact_line.sales_order_key
  ,fact_line.product_key
  ,COALESCE(fact_header.picked_by_person_key,-1) AS picked_by_person_key
  ,COALESCE(fact_header.salesperson_person_key,-1) AS salesperson_person_key
  ,COALESCE(fact_header.contact_person_key,-1) AS contact_person_key
  ,COALESCE(fact_header.customer_key,-1) AS customer_key
  ,FARM_FINGERPRINT(CONCAT(
    COALESCE(fact_header.is_undersupply_backordered,'Undefined')
    , ','
    ,fact_line.package_type_key
  )) AS sales_order_line_indicator_key
  ,fact_header.expected_delivery_date
  ,fact_line.line_picking_completed_when
  ,fact_header.picking_completed_when AS order_picking_completed_when
  ,fact_header.order_date
  ,fact_line.quantity
  ,fact_line.unit_price
  ,fact_line.quantity*fact_line.unit_price AS gross_amount
  ,fact_line.tax_rate AS tax_rate
  ,fact_line.quantity*fact_line.tax_rate AS tax_amount
FROM fact_sales_order_line__cast_type as fact_line 
LEFT JOIN {{ ref ('stg_fact_sales_order') }} as fact_header
ON fact_header.sales_order_key = fact_line.sales_order_key

