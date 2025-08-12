"""
Order service for TPC-C operations
"""

import logging
import os
from typing import Any, Dict, List, Optional

from database.base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class OrderService:
    """Service class for order-related operations"""

    def __init__(
        self, db_connector: BaseDatabaseConnector, region_name: Optional[str] = None
    ):
        self.db = db_connector
        # Get region name from environment variable or use default
        self.region_name = region_name or os.environ.get("REGION_NAME", "default")

    def execute_new_order(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Execute TPC-C New Order transaction"""
        try:
            logger.info(f"Starting New Order transaction: w_id={warehouse_id}, d_id={district_id}, c_id={customer_id}, items={len(items)}")
            
            # Validate inputs
            if not items:
                return {"success": False, "error": "No items provided"}
            
            # Get customer information
            customer_query = """
                SELECT c_first, c_middle, c_last, c_credit, c_discount, c_balance
                FROM customer 
                WHERE c_w_id = @warehouse_id AND c_d_id = @district_id AND c_id = @customer_id
            """
            customer_result = self.db.execute_query(customer_query, {
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id
            })
            
            if not customer_result:
                return {"success": False, "error": "Customer not found"}
            
            customer = customer_result[0]
            
            # Get warehouse and district information
            warehouse_query = """
                SELECT w_tax, w_ytd FROM warehouse WHERE w_id = @warehouse_id
            """
            warehouse_result = self.db.execute_query(warehouse_query, {"warehouse_id": warehouse_id})
            
            if not warehouse_result:
                return {"success": False, "error": "Warehouse not found"}
            
            warehouse = warehouse_result[0]
            
            district_query = """
                SELECT d_tax, d_ytd FROM district WHERE d_w_id = @warehouse_id AND d_id = @district_id
            """
            district_result = self.db.execute_query(district_query, {
                "warehouse_id": warehouse_id,
                "district_id": district_id
            })
            
            if not district_result:
                return {"success": False, "error": "District not found"}
            
            district = district_result[0]
            
            # Get next order ID
            order_id_query = """
                SELECT COALESCE(MAX(o_id), 0) + 1 as next_order_id 
                FROM order_table 
                WHERE o_w_id = @warehouse_id AND o_d_id = @district_id
            """
            order_id_result = self.db.execute_query(order_id_query, {
                "warehouse_id": warehouse_id,
                "district_id": district_id
            })
            
            if not order_id_result:
                return {"success": False, "error": "Failed to get next order ID"}
            
            order_id = order_id_result[0]["next_order_id"]
            
            # Calculate order total
            total_amount = 0
            order_lines = []
            
            for i, item in enumerate(items):
                item_id = item.get("item_id")
                supply_warehouse_id = item.get("supply_warehouse_id", warehouse_id)
                quantity = item.get("quantity", 1)
                
                # Get item information
                item_query = """
                    SELECT i_name, i_price, i_data FROM item WHERE i_id = @item_id
                """
                item_result = self.db.execute_query(item_query, {"item_id": item_id})
                
                if not item_result:
                    return {"success": False, "error": f"Item {item_id} not found"}
                
                item_info = item_result[0]
                
                # Get stock information
                stock_query = """
                    SELECT s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
                           s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt
                    FROM stock 
                    WHERE s_i_id = @item_id AND s_w_id = @supply_warehouse_id
                """
                stock_result = self.db.execute_query(stock_query, {
                    "item_id": item_id,
                    "supply_warehouse_id": supply_warehouse_id
                })
                
                if not stock_result:
                    return {"success": False, "error": f"Stock not found for item {item_id} in warehouse {supply_warehouse_id}"}
                
                stock = stock_result[0]
                
                # Calculate line total
                line_amount = item_info["i_price"] * quantity
                total_amount += line_amount
                
                # Prepare order line data
                order_line = {
                    "ol_o_id": order_id,
                    "ol_d_id": district_id,
                    "ol_w_id": warehouse_id,
                    "ol_number": i + 1,
                    "ol_i_id": item_id,
                    "ol_supply_w_id": supply_warehouse_id,
                    "ol_quantity": quantity,
                    "ol_amount": line_amount,
                    "ol_dist_info": stock[f"s_dist_{district_id:02d}"] if district_id <= 10 else stock["s_dist_01"]
                }
                order_lines.append(order_line)
            
            # Calculate final amounts
            total_amount = total_amount * (1 + district["d_tax"] + warehouse["w_tax"]) * (1 - customer["c_discount"])
            
            # Create the order
            order_data = {
                "o_id": order_id,
                "o_d_id": district_id,
                "o_w_id": warehouse_id,
                "o_c_id": customer_id,
                "o_entry_d": "CURRENT_TIMESTAMP",
                "o_carrier_id": None,
                "o_ol_cnt": len(items),
                "o_all_local": 1 if all(item.get("supply_warehouse_id", warehouse_id) == warehouse_id for item in items) else 0
            }
            
            # For now, we'll simulate the order creation since we can't do transactions
            # In a real implementation, this would be wrapped in a transaction
            logger.info(f"Order {order_id} would be created with total amount: {total_amount:.2f}")
            logger.info(f"Order lines: {len(order_lines)} lines")
            
            return {
                "success": True,
                "order_id": order_id,
                "customer_name": f"{customer['c_first']} {customer['c_middle']} {customer['c_last']}",
                "total_amount": round(total_amount, 2),
                "items_count": len(items),
                "region_created": self.region_name,
                "message": "Order created successfully (simulated - no actual database changes)"
            }
            
        except Exception as e:
            logger.error(f"New order service error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_order_status(
        self, warehouse_id: int, district_id: int, customer_id: int
    ) -> Dict[str, Any]:
        """Get order status for a customer"""
        try:
            return self.db.get_order_status(warehouse_id, district_id, customer_id)
        except Exception as e:
            logger.error(f"Order status service error: {str(e)}")
            return {"success": False, "error": str(e)}

    def execute_delivery(self, warehouse_id: int, carrier_id: int) -> Dict[str, Any]:
        """Execute TPC-C Delivery transaction"""
        try:
            return self.db.execute_delivery(warehouse_id, carrier_id)
        except Exception as e:
            logger.error(f"Delivery service error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_orders(
        self,
        warehouse_id: Optional[int] = None,
        district_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get orders with optional filters and pagination"""
        try:
            return self.db.get_orders(
                warehouse_id=warehouse_id,
                district_id=district_id,
                customer_id=customer_id,
                status=status,
                limit=limit,
                offset=offset,
            )
        except Exception as e:
            logger.error(f"Get orders service error: {str(e)}")
            return {
                "orders": [],
                "total_count": 0,
                "limit": limit,
                "offset": offset,
                "has_next": False,
                "has_prev": False,
            }

    def get_order_details(
        self, warehouse_id: int, district_id: int, order_id: int
    ) -> Dict[str, Any]:
        """Get detailed information about a specific order"""
        try:
            # Get order information
            order_query = """
                SELECT o.*, c.c_first, c.c_middle, c.c_last,
                       CASE WHEN no.no_o_id IS NOT NULL THEN 'New' ELSE 'Delivered' END as status
                FROM order_table o
                JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
                LEFT JOIN new_order no ON no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id
                WHERE o.o_w_id = %s AND o.o_d_id = %s AND o.o_id = %s
            """

            order_result = self.db.execute_query(
                order_query, (warehouse_id, district_id, order_id)
            )

            if not order_result:
                return {"success": False, "error": "Order not found"}

            order = order_result[0]

            # Get order lines
            order_lines_query = """
                SELECT ol.*, i.i_name, i.i_price
                FROM order_line ol
                JOIN item i ON i.i_id = ol.ol_i_id
                WHERE ol.ol_w_id = %s AND ol.ol_d_id = %s AND ol.ol_o_id = %s
                ORDER BY ol.ol_number
            """

            order_lines = self.db.execute_query(
                order_lines_query, (warehouse_id, district_id, order_id)
            )

            # Calculate total amount
            total_amount = sum(float(line.get("ol_amount", 0)) for line in order_lines)

            return {
                "success": True,
                "order": order,
                "order_lines": order_lines,
                "total_amount": total_amount,
            }

        except Exception as e:
            logger.error(f"Get order details service error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_recent_orders(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most recent orders across all warehouses"""
        try:
            query = """
                SELECT o.o_id, o.o_w_id, o.o_d_id, o.o_c_id, o.o_entry_d,
                       c.c_first, c.c_middle, c.c_last,
                       w.w_name,
                       CASE WHEN no.no_o_id IS NOT NULL THEN 'New' ELSE 'Delivered' END as status
                FROM order_table o
                JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
                JOIN warehouse w ON w.w_id = o.o_w_id
                LEFT JOIN new_order no ON no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id
                ORDER BY o.o_entry_d DESC
                LIMIT @limit
            """

            return self.db.execute_query(query, {"limit": limit})

        except Exception as e:
            logger.error(f"Get recent orders service error: {str(e)}")
            return []

    def get_order_statistics(
        self, warehouse_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get order statistics"""
        try:
            stats = {}

            # Base query conditions
            where_clause = "WHERE 1=1"
            params = []

            if warehouse_id:
                where_clause += " AND o_w_id = %s"
                params.append(warehouse_id)

            # Total orders
            total_query = f"SELECT COUNT(*) as count FROM order_table {where_clause}"
            total_result = self.db.execute_query(total_query, tuple(params))
            stats["total_orders"] = total_result[0]["count"] if total_result else 0

            # New orders
            new_query = f"""
                SELECT COUNT(*) as count 
                FROM order_table o 
                JOIN new_order no ON no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id
                {where_clause}
            """
            new_result = self.db.execute_query(new_query, tuple(params))
            stats["new_orders"] = new_result[0]["count"] if new_result else 0

            # Delivered orders
            stats["delivered_orders"] = stats["total_orders"] - stats["new_orders"]

            # Orders today
            today_query = f"""
                SELECT COUNT(*) as count 
                FROM order_table 
                {where_clause} AND DATE(o_entry_d) = CURRENT_DATE
            """
            today_result = self.db.execute_query(today_query, tuple(params))
            stats["orders_today"] = today_result[0]["count"] if today_result else 0

            # Average order value
            avg_query = f"""
                SELECT AVG(total_amount) as avg_amount
                FROM (
                    SELECT SUM(ol_amount) as total_amount
                    FROM order_line ol
                    JOIN order_table o ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id
                    {where_clause.replace("o_w_id", "o.o_w_id")}
                    GROUP BY ol.ol_w_id, ol.ol_d_id, ol.ol_o_id
                ) as order_totals
            """
            avg_result = self.db.execute_query(avg_query, tuple(params))
            stats["avg_order_value"] = (
                float(avg_result[0]["avg_amount"])
                if avg_result and avg_result[0]["avg_amount"]
                else 0.0
            )

            return stats

        except Exception as e:
            logger.error(f"Get order statistics service error: {str(e)}")
            return {}
