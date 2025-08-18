"""
Payment service for TPC-C operations
"""

import logging
from typing import Any, Dict, List, Optional
import psycopg2.extras
from psycopg2.extras import RealDictCursor

from database.base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class PaymentService:
    """Service class for payment-related operations"""

    def __init__(self, db_connector: BaseDatabaseConnector):
        self.db = db_connector
        self.connection = db_connector.connection

    def execute_payment(self, warehouse_id: int, district_id: int, customer_id: int, amount: float):
        """Run payment transaction in DB"""
        try:
            logger.info(f"Inserting payment: W_ID={warehouse_id}, D_ID={district_id}, C_ID={customer_id}, Amount={amount}")

            with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO History (h_w_id, h_c_d_id, h_c_w_id, h_d_id, h_c_id, h_date, h_amount, h_data)
                    VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s)
                    RETURNING *;
                """, (warehouse_id, district_id, warehouse_id, district_id, customer_id, amount, 'Payment transaction'))

                
                self.connection.commit()
                return cur.fetchone()
        except Exception as e:
            self.connection.rollback()
            logger.error(f"DB Payment execution error: {e}", exc_info=True)
            raise


    def get_payment_history(
        self,
        warehouse_id: Optional[int] = None,
        district_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get payment history with optional filters"""
        try:
            return self.db.get_payment_history(
                warehouse_id, district_id, customer_id, limit
            )
        except Exception as e:
            logger.error(f"Get payment history service error: {str(e)}")
            return []


    def get_payment_history_paginated(
        self,
        warehouse_id: Optional[int] = None,
        district_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Fetch payment history from the DB with optional filters & pagination."""

        try:
            query = """
                SELECT h_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data
                FROM History
                WHERE (%(warehouse_id)s IS NULL OR h_w_id = %(warehouse_id)s)
                AND (%(district_id)s IS NULL OR h_c_d_id = %(district_id)s)
                AND (%(customer_id)s IS NULL OR h_c_id = %(customer_id)s)
                ORDER BY h_date DESC
                LIMIT %(limit)s OFFSET %(offset)s
            """
            
            params = {
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "limit": limit,
                "offset": offset
            }

            # Fetch all rows as dicts
            rows = self.db.fetch_all(query, params)

            # Get total count separately
            count_query = """
                SELECT COUNT(*) AS total
                FROM History
                WHERE (%(warehouse_id)s IS NULL OR h_w_id = %(warehouse_id)s)
                AND (%(district_id)s IS NULL OR h_d_id = %(district_id)s)
                AND (%(customer_id)s IS NULL OR h_c_id = %(customer_id)s)
            """
            count_result = self.db.fetch_one(count_query, params)
            total_count = count_result["total"] if count_result else 0

            return {
                "payments": rows,
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": (offset + limit) < total_count,
                "has_prev": offset > 0
            }

        except Exception as e:
            logger.error(f"Error loading payments: {str(e)}")
            return {
                "payments": [],
                "total_count": 0,
                "limit": limit,
                "offset": offset,
                "has_next": False,
                "has_prev": False
            }

    def get_customer_payment_summary(
        self, warehouse_id: int, district_id: int, customer_id: int
    ) -> Dict[str, Any]:
        """Get payment summary for a specific customer"""
        try:
            # Get customer info and payment stats
            customer_query = """
                SELECT c.c_first, c.c_middle, c.c_last, c.c_balance,
                       c.c_ytd_payment, c.c_payment_cnt, c.c_credit,
                       c.c_credit_lim, c.c_discount, c.c_since
                FROM customer c
                WHERE c.c_w_id = %s AND c.c_d_id = %s AND c.c_id = %s
            """

            customer_result = self.db.execute_query(
                customer_query, (warehouse_id, district_id, customer_id)
            )

            if not customer_result:
                return {"success": False, "error": "Customer not found"}

            customer = customer_result[0]

            # Get recent payment history
            history_query = """
                SELECT h.h_date, h.h_amount, h.h_data
                FROM history h
                WHERE h.h_c_w_id = %s AND h.h_c_d_id = %s AND h.h_c_id = %s
                ORDER BY h.h_date DESC
                LIMIT 10
            """

            payment_history = self.db.execute_query(
                history_query, (warehouse_id, district_id, customer_id)
            )

            # Calculate payment statistics
            stats_query = """
                SELECT 
                    COUNT(*) as total_payments,
                    SUM(h_amount) as total_amount,
                    AVG(h_amount) as avg_amount,
                    MIN(h_amount) as min_amount,
                    MAX(h_amount) as max_amount,
                    MIN(h_date) as first_payment,
                    MAX(h_date) as last_payment
                FROM history h
                WHERE h.h_c_w_id = %s AND h.h_c_d_id = %s AND h.h_c_id = %s
            """

            stats_result = self.db.execute_query(
                stats_query, (warehouse_id, district_id, customer_id)
            )
            print(stats_result)

            payment_stats = stats_result[0] if stats_result else {}

            return {
                "success": True,
                "customer": customer,
                "payment_history": payment_history,
                "payment_stats": payment_stats,
            }

        except Exception as e:
            logger.error(f"Get customer payment summary service error: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_payment_history_paginated(
        self,
        warehouse_id: Optional[int] = None,
        district_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Fetch payment history with pagination."""
        try:
            where_clause = """
                WHERE (%(warehouse_id)s IS NULL OR h_w_id = %(warehouse_id)s)
                  AND (%(district_id)s IS NULL OR h_d_id = %(district_id)s)
                  AND (%(customer_id)s IS NULL OR h_c_id = %(customer_id)s)
            """
            params = {
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "limit": limit,
                "offset": offset,
            }

            # Fetch payment rows as dicts
            payments = self.db.fetch_all(
                f"""
                SELECT h_id, h_c_id, h_c_d_id, h_c_w_id, 
                       h_d_id, h_w_id, h_date, h_amount, h_data
                FROM History
                {where_clause}
                ORDER BY h_date DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                params
            )

            # Fetch total count
            total_row = self.db.fetch_one(
                f"SELECT COUNT(*) AS total FROM History {where_clause}",
                params
            )
            total_count = total_row["total"] if total_row else 0

            # Compute total amount here (optional for KPIs)
            # for p in payments:
            #     print(f"Payment Service: {payments}")
            # total_amount = sum(p.get("h_amount", 0) for p in payments)

            return {
                "payments": payments,
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": (offset + limit) < total_count,
                "has_prev": offset > 0,
                # "total_amount": total_amount
            }

        except Exception as e:
            logger.error(f"Error loading payments: {e}", exc_info=True)
            return {
                "payments": [],
                "total_count": 0,
                "limit": limit,
                "offset": offset,
                "has_next": False,
                "has_prev": False,
                "total_amount": 0
            }

    def get_recent_payments(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get most recent payments across all warehouses"""
        try:
            query = """
                SELECT h.h_date, h.h_amount, h.h_data,
                       h.h_c_id, h.h_c_w_id, h.h_c_d_id,
                       c.c_first, c.c_middle, c.c_last,
                       w.w_name
                FROM history h
                JOIN customer c ON c.c_w_id = h.h_c_w_id AND c.c_d_id = h.h_c_d_id AND c.c_id = h.h_c_id
                JOIN warehouse w ON w.w_id = h.h_c_w_id
                ORDER BY h.h_date DESC
                LIMIT %s
            """

            return self.db.execute_query(query, (limit,))

        except Exception as e:
            logger.error(f"Get recent payments service error: {str(e)}")
            return []

    def get_payment_trends(
        self, warehouse_id: Optional[int] = None, days: int = 30
    ) -> Dict[str, Any]:
        """Get payment trends over specified number of days"""
        try:
            # Base query conditions
            where_clause = "WHERE h_date >= CURRENT_DATE - INTERVAL '%s days'"
            params = [days]

            if warehouse_id:
                where_clause += " AND h_w_id = %s"
                params.append(warehouse_id)

            # Daily payment trends
            daily_query = f"""
                SELECT DATE(h_date) as payment_date,
                       COUNT(*) as payment_count,
                       SUM(h_amount) as total_amount,
                       AVG(h_amount) as avg_amount
                FROM history
                {where_clause}
                GROUP BY DATE(h_date)
                ORDER BY payment_date DESC
            """

            daily_trends = self.db.execute_query(daily_query, tuple(params))

            # Payment amount distribution
            distribution_query = f"""
                SELECT 
                    COUNT(CASE WHEN h_amount < 100 THEN 1 END) as under_100,
                    COUNT(CASE WHEN h_amount >= 100 AND h_amount < 500 THEN 1 END) as between_100_500,
                    COUNT(CASE WHEN h_amount >= 500 AND h_amount < 1000 THEN 1 END) as between_500_1000,
                    COUNT(CASE WHEN h_amount >= 1000 THEN 1 END) as over_1000
                FROM history
                {where_clause}
            """

            distribution_result = self.db.execute_query(
                distribution_query, tuple(params)
            )
            distribution = distribution_result[0] if distribution_result else {}

            return {
                "success": True,
                "daily_trends": daily_trends,
                "amount_distribution": distribution,
                "period_days": days,
            }

        except Exception as e:
            logger.error(f"Get payment trends service error: {str(e)}")
            return {"success": False, "error": str(e)}

    def validate_payment_data(
        self, warehouse_id: int, district_id: int, customer_id: int, amount: float
    ) -> Dict[str, Any]:
        """Validate payment data before processing"""
        try:
            errors = []

            # Validate amount
            if amount <= 0:
                errors.append("Payment amount must be positive")

            if amount > 10000:  # Arbitrary large amount check
                errors.append("Payment amount exceeds maximum allowed")

            # Check if customer exists
            customer_query = """
                SELECT c_id, c_first, c_last, c_balance, c_credit_lim
                FROM customer
                WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
            """

            customer_result = self.db.execute_query(
                customer_query, (warehouse_id, district_id, customer_id)
            )

            if not customer_result:
                errors.append("Customer not found")
            else:
                customer = customer_result[0]
                # Check if payment would exceed credit limit (if applicable)
                new_balance = customer["c_balance"] - amount
                if new_balance < -customer["c_credit_lim"]:
                    errors.append("Payment would exceed customer credit limit")

            # Check if warehouse and district exist
            district_query = """
                SELECT d_id, w.w_name
                FROM district d
                JOIN warehouse w ON w.w_id = d.d_w_id
                WHERE d.d_w_id = %s AND d.d_id = %s
            """

            district_result = self.db.execute_query(
                district_query, (warehouse_id, district_id)
            )

            if not district_result:
                errors.append("Warehouse or district not found")

            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "customer": customer_result[0] if customer_result else None,
                "district": district_result[0] if district_result else None,
            }

        except Exception as e:
            logger.error(f"Payment validation service error: {str(e)}")
            return {"valid": False, "errors": [str(e)]}
