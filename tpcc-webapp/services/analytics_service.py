"""
Study Analytics Service - Simplified version for UX study
Uses skeleton connectors that participants will implement
"""

import logging
from typing import Any, Dict

# from database.base_connector import create_study_connector

from database.neon_connector import create_study_connector

# from database.connector_factory import create_study_connector

logger = logging.getLogger(__name__)


class AnalyticsService:
    """
    Simplified analytics service for UX study

    This service provides basic dashboard metrics using the skeleton connectors
    that participants will implement during the study.
    """

    # def __init__(self, db_connector):
    #     self.db_connector = db_connector
    #     self.connector = db_connector  # 👈 alias for backward compatibility
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.connector = db_connector  # alias for backward compatibility
        self.db = db_connector 
        
    # def __init__(self):
    #     """Initialize the study analytics service"""
    #     self.connector = None
    #     self._initialize_connector()

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
            return {
                "error": "No database connector available",
                "metrics": self._get_default_metrics(),
            }

        try:
            # Test connection first
            if not self.connector.test_connection():
                return {
                    "error": "Database connection failed",
                    "metrics": self._get_default_metrics(),
                }

            # Try to get basic metrics using simple queries
            metrics = {}

            # Get warehouse count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM warehouse"
                )
                if result and "count" in result[0]:
                    warehouse_count = result[0]["count"]
                else:
                    warehouse_count = 0
                
                print(f"Total warehouses: {warehouse_count}")
                metrics["total_warehouses"] = warehouse_count
                # metrics["total_warehouses"] = result[0]["count"] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get warehouse count: {str(e)}")
                metrics["total_warehouses"] = 0

            # Get customer count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM customer"
                )
                if result and "count" in result[0]:
                    customer_count = result[0]["count"]
                else:
                    customer_count = 0
                # print("Customer_count: {customer_count}") 
                metrics["total_customers"] = customer_count
            except Exception as e:
                logger.warning(f"Failed to get customer count: {str(e)}")
                metrics["total_customers"] = 0

            # Get order count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM orders"
                )
                # print(result) 
                metrics["total_orders"] = result[0]["count"] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get order count: {str(e)}")
                metrics["total_orders"] = 0

            # Get item count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM item"
                )
                # print(result) 
                metrics["total_items"] = result[0]["count"] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get item count: {str(e)}")
                metrics["total_items"] = 0

            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "metrics": metrics,
            }

        except Exception as e:
            logger.error(f"Failed to get dashboard metrics: {str(e)}")
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
                FROM orders 
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
            "total_warehouses": 10,
            "total_customers": 10,
            "total_orders": 10,
            "total_items": 10,
            "new_orders": 10,
            "low_stock_items": 0,
            "orders_last_24h": 0,
            "avg_order_value": 0.0,
        }

    def get_warehouses(
        self
        ) -> Dict[str, Any]:
        """Get order status for a customer"""
        try:
            query ="Select * from Warehouse"
            result = self.connector.execute_query(query)
            return {"success": True, "warehouses": result}
        except Exception as e:
            logger.error(f"Order status service error: {str(e)}")
            return {"success": False, "error": str(e)}

    def close(self):
        """Close database connections"""
        if self.connector:
            try:
                self.connector.close_connection()
                logger.info("📊 Study Analytics Service connections closed")
            except Exception as e:
                logger.error(f"Error closing connections: {str(e)}")
