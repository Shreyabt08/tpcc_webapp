"""
Order service for TPC-C operations
"""

import logging
import os
from typing import Any, Dict, List, Optional

from database.base_connector import BaseDatabaseConnector
from datetime import datetime

logger = logging.getLogger(__name__)


class OrderService:
    """Service class for order-related operations"""


    def __init__(
        self, db_connector: BaseDatabaseConnector, region_name: Optional[str] = None
    ):
        self.db = db_connector
        # Get region name from environment variable or use default
        self.region_name = region_name or os.environ.get("REGION_NAME", "us-east-1")

    def execute_delivery(self, warehouse_id: int, carrier_id: int):
        """
        Executes the TPC-C delivery transaction for a warehouse.
        - warehouse_id: Warehouse ID
        - carrier_id: Carrier ID (1–10)
        """

        print(f"🚚 Starting delivery process for Warehouse {warehouse_id} with Carrier {carrier_id}")

        for district_id in range(1, 11):
            print(f"\n📦 Processing District {district_id}")

            # 1. Get the oldest undelivered order
            orders = self.db.execute_query("""
                SELECT o_id, o_c_id
                FROM orders
                WHERE o_w_id = %s
                AND o_d_id = %s
                AND o_carrier_id IS NULL
                ORDER BY o_id ASC
                LIMIT 1
            """, (warehouse_id, district_id))

            if not orders:
                print("   ➡ No undelivered orders found for this district.")
                continue

            order_id = orders[0]['o_id']
            customer_id = orders[0]['o_c_id']
            print(f"   📝 Found Order ID {order_id} for Customer {customer_id}")

            # 2. Update order's carrier ID
            self.db.execute_query("""
                UPDATE orders
                SET o_carrier_id = %s
                WHERE o_w_id = %s
                AND o_d_id = %s
                AND o_id = %s
            """, (carrier_id, warehouse_id, district_id, order_id))
            print(f"   ✅ Updated carrier for Order {order_id}")

            # 3. Update order lines' delivery date
            self.db.execute_query("""
                UPDATE order_line
                SET ol_delivery_d = CURRENT_TIMESTAMP
                WHERE ol_w_id = %s
                AND ol_d_id = %s
                AND ol_o_id = %s
            """, (warehouse_id, district_id, order_id))
            print(f"   📅 Set delivery date for Order {order_id}")

            # 4. Calculate total amount from order lines
            total_amount_result = self.db.execute_query("""
                SELECT SUM(ol_amount) AS total_amount
                FROM order_line
                WHERE ol_w_id = %s
                AND ol_d_id = %s
                AND ol_o_id = %s
            """, (warehouse_id, district_id, order_id))

            total_amount = total_amount_result[0]['total_amount'] if total_amount_result else 0
            print(f"   💰 Total amount for Order {order_id}: {total_amount}")

            # 5. Update customer balance and delivery count
            self.db.execute_query("""
                UPDATE customer
                SET c_balance = c_balance + %s,
                    c_delivery_cnt = c_delivery_cnt + 1
                WHERE c_w_id = %s
                AND c_d_id = %s
                AND c_id = %s
            """, (total_amount, warehouse_id, district_id, customer_id))
            print(f"   👤 Updated Customer {customer_id} balance and delivery count")

        # Commit changes after all updates
        self.db.connection.commit()
        print("\n✅ Delivery process completed successfully.")


    def execute_new_order(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Execute TPC-C New Order transaction:
        1. Insert into orders
        2. Insert order_lines for each item
        3. Mark order as multi-region if items come from different warehouses
        """
        try:
            logger.info(
                f"🛒 Creating new order: warehouse_id={warehouse_id}, district_id={district_id}, "
                f"customer_id={customer_id}, items={len(items)}"
            )
            region_created = os.getenv("REGION_NAME", "default")  
 
            # 🔍 Validate item fields
            for idx, item in enumerate(items, 1):
                print(f"🔍 Validating item {idx}: {item}")
                if "i_id" not in item or "quantity" not in item:
                    raise ValueError(f"Item {idx} is missing required fields: {item}")
                logger.info(f"📦 Item {idx}: {item}")

            # Ensure warehouse_id and district_id are ints
            warehouse_id = int(warehouse_id)
            district_id = int(district_id)

            # Convert item warehouse_id to int if present, else default
            for item in items:
                item['warehouse_id'] = int(item.get('warehouse_id', warehouse_id))

            # ✅ Count local vs remote items
            local_items = sum(
                1 for item in items if item.get("warehouse_id", warehouse_id) == warehouse_id
            )
            all_local = 1 if local_items == len(items) else 0
            
            # 🆔 Generate new order ID
            order_id_result = self.db.fetch_one(
                """
                SELECT COALESCE(MAX(o_id), 0) + 1 AS next_order_id
                FROM orders
                WHERE o_w_id = %s AND o_d_id = %s
                """,
                (warehouse_id, district_id),
            )
            if not order_id_result or 'next_order_id' not in order_id_result:
                raise RuntimeError("Failed to fetch new order id")

            order_id = order_id_result['next_order_id']

            # 📝 Insert into orders table
            order_entry_d = datetime.utcnow()
            self.db.execute_query(
                """
                INSERT INTO orders (
                    o_id, o_d_id, o_w_id, o_c_id,
                    o_entry_d, o_carrier_id, o_ol_cnt, o_all_local,region_created
                ) VALUES (%s, %s, %s, %s, %s, NULL, %s, %s,%s)
                """,
                (
                    order_id,
                    district_id,
                    warehouse_id,
                    customer_id,
                    order_entry_d,
                    len(items),
                    all_local,
                    region_created
                ),
            )

            # 📥 Insert order_lines
            for line_number, item in enumerate(items, 1):
                item_id = item["i_id"]
                quantity = item["quantity"]
                supply_w_id = item.get("warehouse_id", warehouse_id)

                # Get item price
                price_result = self.db.fetch_one(
                    "SELECT i_price FROM item WHERE i_id = %s", (item_id,)
                )
                if not price_result or "i_price" not in price_result:
                    raise RuntimeError(f"Item price not found for item_id {item_id}")
                item_price = price_result["i_price"]

                amount = quantity * item_price
                ol_dist_info = "DEFAULT DIST INFO".ljust(24)[:24]  # exactly 24 chars

                self.db.execute_query(
                    """
                    INSERT INTO order_line (
                        ol_o_id, ol_d_id, ol_w_id, ol_number,
                        ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d, ol_dist_info
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s)
                    """,
                    (
                        order_id,
                        district_id,
                        warehouse_id,
                        line_number,
                        item_id,
                        supply_w_id,
                        quantity,
                        amount,
                        ol_dist_info,
                    ),
                )

            logger.info(f"✅ Order {order_id} created successfully")

            return {
                "success": True,
                "order_id": order_id,
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "items_count": len(items),
                "all_local": all_local,
            }

        except Exception as e:
            import traceback
            error_message = str(e) or "Unexpected error"
            logger.error(f"❌ New order service error: {error_message}\n{traceback.format_exc()}")
            return {"success": False, "error": error_message}

    def get_orders(self, warehouse_id=None, district_id=None, customer_id=None, status=None, limit=50, offset=0):
        try:
            query = """
                SELECT *,
                        CASE 
                            WHEN o_carrier_id IS NULL THEN 'New'
                            ELSE 'Delivered'
                        END AS status
                    FROM orders
                    WHERE (%(warehouse_id)s IS NULL OR o_w_id = %(warehouse_id)s)
                    AND (%(district_id)s IS NULL OR o_d_id = %(district_id)s)
                    AND (%(customer_id)s IS NULL OR o_c_id = %(customer_id)s)
                    AND (
                            %(status)s IS NULL
                            OR (%(status)s = 'New'   AND o_carrier_id IS NULL)
                            OR (%(status)s = 'Delivered' AND o_carrier_id IS NOT NULL)
                        )
                    ORDER BY o_entry_d DESC
                    LIMIT %(limit)s OFFSET %(offset)s
            """
            params = {
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "status": status,
                "limit": limit,
                "offset": offset,
            }

            with self.db.cursor(dictionary=True) as cursor:
                cursor.execute(query, params)
                orders = cursor.fetchall()

            count_query = """
                SELECT COUNT(*) AS total
                FROM orders
                WHERE (%(warehouse_id)s IS NULL OR o_w_id = %(warehouse_id)s)
                AND (%(district_id)s IS NULL OR o_d_id = %(district_id)s)
                AND (%(customer_id)s IS NULL OR o_c_id = %(customer_id)s)
                AND (%(status)s IS NULL OR 
                    (%(status)s = 'New' AND o_carrier_id IS NULL) OR
                    (%(status)s = 'Delivered' AND o_carrier_id IS NOT NULL))
            """
            with self.db.cursor(dictionary=True) as cursor:
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()["total"]
            return {
                "orders": orders,
                "total_count": total_count,
                "has_prev": offset > 0,
                "has_next": offset + limit < total_count
            }

        except Exception as e:
            logger.error(f"Error in get_orders: {str(e)}")
            raise

    def get_order_status(self, warehouse_id, district_id, customer_id):
        # Fetch latest order for the customer
        order_query = """
            SELECT o_id, o_entry_d, o_carrier_id, c_first, c_middle, c_last, c_balance
            FROM orders
            JOIN customer ON o_w_id = c_w_id AND o_d_id = c_d_id AND o_c_id = c_id
            WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s
            ORDER BY o_id DESC LIMIT 1
        """
        order = self.db.fetch_one(order_query, (warehouse_id, district_id, customer_id))
        if not order:
            return {"success": False, "error": "No order found"}

        # Fetch order lines
        lines_query = """
            SELECT ol_i_id, i_name, ol_quantity, ol_amount, ol_supply_w_id, ol_delivery_d
            FROM order_line
            JOIN item ON ol_i_id = i_id
            WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s
        """
        order_lines = self.db.fetch_all(lines_query, (warehouse_id, district_id, order['o_id']))

        return {
            "success": True,
            "customer_name": f"{order['c_first']} {order['c_middle']} {order['c_last']}",
            "customer_balance": order['c_balance'],
            "order_id": order['o_id'],
            "order_date": order['o_entry_d'],
            "carrier_id": order['o_carrier_id'],
            "order_line_count": len(order_lines),
            "order_lines": order_lines
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
                FROM orders o
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
                FROM orders o
                JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
                JOIN warehouse w ON w.w_id = o.o_w_id
                LEFT JOIN new_order no ON no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id
                ORDER BY o.o_entry_d DESC
                LIMIT %s
            """

            return self.db.execute_query(query, (limit,))

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
            total_query = f"SELECT COUNT(*) as count FROM orders {where_clause}"
            total_result = self.db.execute_query(total_query, tuple(params))
            stats["total_orders"] = total_result[0]["count"] if total_result else 0

            # New orders
            new_query = f"""
                SELECT COUNT(*) as count 
                FROM orders o 
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
                FROM orders 
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
                    JOIN orders o ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id
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

   