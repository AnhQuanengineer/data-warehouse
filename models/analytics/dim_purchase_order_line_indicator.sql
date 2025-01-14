WITH dim_is_order AS(
  SELECT
    TRUE AS is_order_finalized_boolean
    , 'Order Finalized' AS is_order_finalized
    ,TRUE AS is_order_finalized_line_boolean
    , 'Order Finalized Line' AS is_order_finalized_line

  UNION ALL
  SELECT
    FALSE AS is_order_finalized_boolean
    , 'Not Order Finalized' AS is_order_finalized
    ,FALSE AS is_order_finalized_line_boolean
    , 'Not Order Finalized Line' AS is_order_finalized_line
)
SELECT 
  FARM_FINGERPRINT(
    CONCAT(dim_is_order.is_order_finalized, ',' ,dim_is_order.is_order_finalized_line , ',' , dim_package_type.package_type_key, ',' , dim_delivery_method.delivery_method_key)
    ) AS purchase_order_line_indicator_key
  ,dim_is_order.is_order_finalized_boolean
  ,dim_is_order.is_order_finalized
  ,dim_is_order.is_order_finalized_line_boolean
  ,dim_is_order.is_order_finalized_line
  ,dim_package_type.package_type_key
  ,dim_package_type.package_type_name
  ,dim_delivery_method.delivery_method_key
  ,dim_delivery_method.delivery_method_name
FROM dim_is_order 
CROSS JOIN {{ ref('stg_dim_package_type') }} AS dim_package_type
CROSS JOIN {{ ref('stg_dim_delivery_method') }} AS dim_delivery_method
