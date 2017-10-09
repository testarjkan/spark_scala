package retail.dataframes

case class OrderItems(
    order_item_id: Int,
    order_item_order_id: Int,
    order_item_product_id: Int,
    order_item_qty: Int,
    order_item_subtotal: Float,
    order_item_price: Float
)
