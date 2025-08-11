"""
Study Analytics Service - Simplified version for UX study
Uses skeleton connectors that participants will implement
"""

import logging
from typing import Any, Dict

from database.connector_factory import create_study_connector

logger = logging.getLogger(__name__)


class AnalyticsService:
    """
    Simplified analytics service for UX study

    This service provides basic dashboard metrics using the skeleton connectors
    that participants will implement during the study.
    """

    def __init__(self, db_connector=None):
        """Initialize the study analytics service"""
        if db_connector:
            self.connector = db_connector
        else:
            self.connector = None
            self._initialize_connector()

    def _initialize_connector(self):
        """Initialize the database connector for the study"""
        try:
            self.connector = create_study_connector()
            logger.info(
                f"📊 Study Analytics Service initialized with {self.connector.get_provider_name()}"
            )
        except Exception as e:
            logger.error(f"❌ Failed to initialize study connector: {str(e)}")
            self.connector = None

    def test_connection(self) -> Dict[str, Any]:
        """
        Test database connection

        Returns:
            dict: Connection test results
        """
        if not self.connector:
            return {
                "success": False,
                "error": "No database connector available",
                "provider": "Unknown",
            }

        try:
            success = self.connector.test_connection()
            return {
                "success": success,
                "provider": self.connector.get_provider_name(),
                "message": "Connection successful" if success else "Connection failed",
            }
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "provider": self.connector.get_provider_name(),
            }

    def get_dashboard_metrics(self) -> Dict[str, Any]:
        """
        Get dashboard metrics for the study webapp

        Returns:
            dict: Dashboard metrics or error information
        """
        if not self.connector:
            logger.error("❌ No database connector available")
            print("❌ No database connector available")
            default_metrics = self._get_default_metrics()
            print("📊 Using default metrics (no database connection):")
            print("-" * 40)
            for key, value in default_metrics.items():
                print(f"   {key}: {value}")
            print("-" * 40)
            return {
                "error": "No database connector available",
                "metrics": default_metrics,
            }

        try:
            # Test connection first
            connection_test = self.connector.test_connection()
            if not connection_test:
                logger.error("❌ Database connection failed")
                print("❌ Database connection failed")
                default_metrics = self._get_default_metrics()
                print("📊 Using default metrics (connection failed):")
                print("-" * 40)
                for key, value in default_metrics.items():
                    print(f"   {key}: {value}")
                print("-" * 40)
                return {
                    "error": "Database connection failed",
                    "metrics": default_metrics,
                }
            
            logger.info("✅ Database connection successful")
            print("✅ Database connection successful")

            # Try to get basic metrics using simple queries
            metrics = {}

            # Get warehouse count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM warehouse"
                )
                warehouse_count = result[0]["count"] if result and len(result) > 0 else 0
                metrics["total_warehouses"] = warehouse_count
            except Exception as e:
                logger.warning(f"Failed to get warehouse count: {str(e)}")
                metrics["total_warehouses"] = 0

            # Get customer count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM customer"
                )
                customer_count = result[0]["count"] if result and len(result) > 0 else 0
                metrics["total_customers"] = customer_count
            except Exception as e:
                logger.warning(f"Failed to get customer count: {str(e)}")
                metrics["total_customers"] = 0

            # Get order count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM order_table"
                )
                order_count = result[0]["count"] if result and len(result) > 0 else 0
                metrics["total_orders"] = order_count
            except Exception as e:
                logger.warning(f"Failed to get order count: {str(e)}")
                metrics["total_orders"] = 0

            # Get item count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM item"
                )
                item_count = result[0]["count"] if result and len(result) > 0 else 0
                metrics["total_items"] = item_count
            except Exception as e:
                logger.warning(f"Failed to get item count: {str(e)}")
                metrics["total_items"] = 0

            # Get additional metrics for dashboard
            # New orders (orders with o_carrier_id IS NULL)
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM order_table WHERE o_carrier_id IS NULL"
                )
                new_orders_count = result[0]["count"] if result and len(result) > 0 else 0
                metrics["new_orders"] = new_orders_count
            except Exception as e:
                logger.warning(f"Failed to get new orders count: {str(e)}")
                metrics["new_orders"] = 0

            # Low stock items (stock with quantity < 50)
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM stock WHERE s_quantity < 50"
                )
                low_stock_count = result[0]["count"] if result and len(result) > 0 else 0
                metrics["low_stock_items"] = low_stock_count
            except Exception as e:
                logger.warning(f"Failed to get low stock items count: {str(e)}")
                metrics["low_stock_items"] = 0

            # Orders in last 24 hours (simplified - just get recent orders)
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM order_table ORDER BY o_entry_d DESC LIMIT 100"
                )
                recent_orders_count = result[0]["count"] if result and len(result) > 0 else 0
                metrics["orders_last_24h"] = recent_orders_count
            except Exception as e:
                logger.warning(f"Failed to get recent orders count: {str(e)}")
                metrics["orders_last_24h"] = 0

            # Average order value (simplified)
            metrics["avg_order_value"] = 0.0  # TODO: Implement actual calculation

            logger.info("🎉 All dashboard metrics retrieved successfully")
            
            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "metrics": metrics,
            }

        except Exception as e:
            logger.error(f"Failed to get dashboard metrics: {str(e)}")
            print(f"❌ Failed to get dashboard metrics: {str(e)}")
            return {
                "error": str(e),
                "provider": self.connector.get_provider_name()
                if self.connector
                else "Unknown",
                "metrics": self._get_default_metrics(),
            }

    def get_orders(self, limit: int = 10) -> Dict[str, Any]:
        """
        Get recent orders for the study webapp

        Args:
            limit: Maximum number of orders to return

        Returns:
            dict: Orders data or error information
        """
        if not self.connector:
            return {"error": "No database connector available", "orders": []}

        try:
            if not self.connector.test_connection():
                return {"error": "Database connection failed", "orders": []}

            query = f"""
                SELECT o_id, o_w_id, o_d_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local
                FROM order_table 
                ORDER BY o_entry_d DESC 
                LIMIT {limit}
            """

            result = self.connector.execute_query(query)

            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "orders": result,
            }

        except Exception as e:
            logger.error(f"Failed to get orders: {str(e)}")
            return {
                "error": str(e),
                "provider": self.connector.get_provider_name()
                if self.connector
                else "Unknown",
                "orders": [],
            }

    def get_warehouses(self) -> list:
        """
        Get list of warehouses for dropdown filters

        Returns:
            list: List of warehouse dictionaries or empty list if error
        """
        if not self.connector:
            return []

        try:
            if not self.connector.test_connection():
                return []

            query = """
                SELECT w_id, w_name, w_city, w_state
                FROM warehouse 
                ORDER BY w_id
            """

            result = self.connector.execute_query(query)

            # Convert to list of dictionaries with proper keys
            warehouses = []
            for row in result:
                warehouses.append({
                    "w_id": row.get("w_id", row.get("count")),
                    "w_name": row.get("w_name", f"Warehouse {row.get('w_id', row.get('count'))}"),
                    "w_city": row.get("w_city", "Unknown"),
                    "w_state": row.get("w_state", "Unknown")
                })

            return warehouses

        except Exception as e:
            logger.error(f"Failed to get warehouses: {str(e)}")
            return []

    def get_inventory(self, limit: int = 10) -> Dict[str, Any]:
        """
        Get inventory data for the study webapp

        Args:
            limit: Maximum number of items to return

        Returns:
            dict: Inventory data or error information
        """
        if not self.connector:
            return {"error": "No database connector available", "inventory": []}

        try:
            if not self.connector.test_connection():
                return {"error": "Database connection failed", "inventory": []}

            query = f"""
                SELECT s.s_i_id, i.i_name, s.s_w_id, s.s_quantity, i.i_price
                FROM stock s
                JOIN item i ON s.s_i_id = i.i_id
                WHERE s.s_quantity < 50
                ORDER BY s.s_quantity ASC
                LIMIT {limit}
            """

            result = self.connector.execute_query(query)

            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "inventory": result,
            }

        except Exception as e:
            logger.error(f"Failed to get inventory: {str(e)}")
            return {
                "error": str(e),
                "provider": self.connector.get_provider_name()
                if self.connector
                else "Unknown",
                "inventory": [],
            }

    def _get_default_metrics(self) -> Dict[str, int]:
        """Get default metrics when database is not available"""
        return {
            "total_warehouses": 0,
            "total_customers": 0,
            "total_orders": 0,
            "total_items": 0,
            "new_orders": 0,
            "low_stock_items": 0,
            "orders_last_24h": 0,
            "avg_order_value": 0.0,
        }

    def close(self):
        """Close database connections"""
        if self.connector:
            try:
                self.connector.close_connection()
                logger.info("📊 Study Analytics Service connections closed")
            except Exception as e:
                logger.error(f"Error closing connections: {str(e)}")
