"""
TPC-C Flask Web Application
NeonDB TPC-C Web Application
"""

import logging
import os
from datetime import datetime
from datetime import datetime
import time

# Load environment variables from .env file
from dotenv import load_dotenv

load_dotenv()

# Import database connectors and ORM
from database.neon_connector import NeonConnector
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for
from services.analytics_service import AnalyticsService
from services.inventory_service import InventoryService
from services.order_service import OrderService
from services.payment_service import PaymentService
from flask_sqlalchemy import SQLAlchemy
        



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ORM is not available - using raw SQL only
orm_available = False

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-production")

# Set up SQLAlchemy
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("NEON_CONNECTION_STRING")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
# Global services
db_connector = None
orm_session = None
order_service = None
inventory_service = None
payment_service = None
analytics_service = None


def initialize_services():
    """Initialize database connection and services"""
    global         db_connector,         orm_session,         order_service,         inventory_service,         payment_service,         analytics_service

    try:
        # Create database connector
        db_connector = NeonConnector()
        
        # ORM is not available - using raw SQL only
        orm_session = None

        # Get region name from environment
        region_name = os.environ.get("REGION_NAME")

        # Initialize services without ORM session
        order_service = OrderService(db_connector, region_name)
        inventory_service = InventoryService(db_connector)
        payment_service = PaymentService(db_connector)
        analytics_service = AnalyticsService(db_connector)

        logger.info("Services initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize services: {str(e)}")
        raise

# Initialize services at startup
with app.app_context():
    initialize_services()


@app.route("/")
def dashboard():
    """Main dashboard showing key metrics"""
    try:
        logger.info("🏠 Dashboard page accessed prod")
        logger.info(f"   Database Provider: {db_connector.get_provider_name()}")
        logger.info(f"   ORM Available: {orm_available}")

        # Get dashboard metrics
        logger.info("   Fetching dashboard metrics...")
        # breakpoint()
        dashboard_data = analytics_service.get_dashboard_metrics()
        metrics = dashboard_data.get("metrics", {})
        logger.info(f"   ✅ Dashboard metrics retrieved: {len(metrics)} metrics")
        return render_template(
            "dashboard.html", metrics=metrics, provider=db_connector.get_provider_name()
        )
    except Exception as e:
        logger.error(f"❌ Dashboard error: {str(e)}")
        flash(f"Error loading dashboard: {str(e)}", "error")
        return render_template(
            "dashboard.html",
            metrics={},
            provider=db_connector.get_provider_name() if db_connector else "Unknown",
        )


@app.route("/orders")
def orders():
    """Order management page"""
    try:
        logger.info("📋 Orders page accessed")

        # Get filter parameters
        warehouse_id = request.args.get("warehouse_id", type=int)
        district_id = request.args.get("district_id", type=int)
        customer_id = request.args.get("customer_id", type=int)
        status = request.args.get("status")
        limit = request.args.get("limit", 50, type=int)
        page = request.args.get("page", 1, type=int)

        # Calculate offset
        offset = (page - 1) * limit

        logger.info(
            f"   Filters: warehouse_id={warehouse_id}, district_id={district_id}, customer_id={customer_id}, status={status}, limit={limit}, page={page}"
        )

        # Get orders with filters and pagination
        logger.info("   Fetching orders data...")
        orders_result = order_service.get_orders(
            warehouse_id=warehouse_id,
            district_id=district_id,
            customer_id=customer_id,
            status=status,
            limit=limit,
            offset=offset,
        )
        logger.info(
            f"   ✅ Retrieved {len(orders_result.get('orders', []))} orders out of {orders_result.get('total_count', 0)} total"
        )

        # Get warehouses for filter dropdown
        logger.info("   Fetching warehouses for dropdown...")
        warehouses = analytics_service.get_warehouses()
        logger.info(f"   ✅ Retrieved {len(warehouses)} warehouses")

         # Get districts for filter dropdown
        logger.info("   Fetching districts for dropdown...")
        districts = analytics_service.get_districts()
        logger.info(f"✅ Retrieved {len(districts)} districts")

        # Calculate pagination info
        total_count = orders_result.get("total_count", 0)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        pagination = {
            "page": page,
            "limit": limit,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_prev": orders_result.get("has_prev", False),
            "has_next": orders_result.get("has_next", False),
            "prev_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if page < total_pages else None,
            "start_item": offset + 1 if total_count > 0 else 0,
            "end_item": min(offset + limit, total_count),
        }

        return render_template(
            "orders.html",
            orders=orders_result.get("orders", []),
            warehouses=warehouses,
            districts= districts,
            pagination=pagination,
            filters={
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "status": status,
                "limit": limit,
            },
        )
    except Exception as e:
        logger.error(f"❌ Orders page error: {str(e)}")
        flash(f"Error loading orders: {str(e)}", "error")

        # Ensure we have proper default values for template variables
        warehouse_id = request.args.get("warehouse_id", type=int)
        district_id = request.args.get("district_id", type=int)
        customer_id = request.args.get("customer_id", type=int)
        status = request.args.get("status")
        limit = request.args.get("limit", 50, type=int)

        return render_template(
            "orders.html",
            orders=[],
            warehouses=[],
            pagination={
                "page": 1,
                "limit": limit,
                "total_count": 0,
                "total_pages": 1,
                "has_prev": False,
                "has_next": False,
                "prev_page": None,
                "next_page": None,
                "start_item": 0,
                "end_item": 0,
            },
            filters={
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "status": status,
                "limit": limit,
            },
        )

@app.route("/inventory")
def inventory():
    """Inventory management page"""
    try:
        logger.info("📦 Inventory page accessed")

        # Get filter parameters from query
        warehouse_id = request.args.get("warehouse_id", type=int)
        low_stock_threshold = request.args.get("threshold", 10, type=int)
        item_search = request.args.get("item_search")
        limit = request.args.get("limit", 100, type=int)
        page = request.args.get("page", 1, type=int)

        offset = (page - 1) * limit

        logger.info(
            f"Filters: warehouse_id={warehouse_id}, threshold={low_stock_threshold}, "
            f"search='{item_search or ''}', limit={limit}, page={page}"
        )

        # Fetch inventory data with pagination
        inventory_result = inventory_service.get_inventory_paginated(
            warehouse_id=warehouse_id,
            low_stock_threshold=low_stock_threshold,
            item_search=item_search,
            limit=limit,
            offset=offset,
        )
        inventory_list = inventory_result.get("inventory", [])
        total_count = inventory_result.get("total_count", 0)
        logger.info(f"Retrieved {len(inventory_list)} inventory items out of {total_count} total")

        # Get warehouses and districts for filters
        warehouses = analytics_service.get_warehouses()
        districts = analytics_service.get_districts()
      
        # Calculate total inventory value (safe key access)
        total_value = sum(item.get('s_quantity', 0) for item in inventory_list)
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        pagination = {
            "page": page,
            "limit": limit,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_prev": inventory_result.get("has_prev", False),
            "has_next": inventory_result.get("has_next", False),
            "prev_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if page < total_pages else None,
            "start_item": offset + 1 if total_count > 0 else 0,
            "end_item": min(offset + limit, total_count),
        }

        filters = {
            "warehouse_id": warehouse_id,
            "threshold": low_stock_threshold,
            "item_search": item_search,
            "limit": limit,
        }

        return render_template(
            "inventory.html",
            inventory=inventory_list,
            total_value=total_value,
            warehouses=warehouses,
            districts=districts,
            pagination=pagination,
            filters=filters,
        )

    except Exception as e:
        logger.error(f"Inventory page error: {str(e)}")
        flash(f"Error loading inventory: {str(e)}", "error")

        # Provide safe defaults
        default_filters = {"warehouse_id": None, "threshold": 10, "item_search": "", "limit": 100}
        default_pagination = {
            "page": 1,
            "limit": 100,
            "total_count": 0,
            "total_pages": 1,
            "has_prev": False,
            "has_next": False,
            "prev_page": None,
            "next_page": None,
            "start_item": 0,
            "end_item": 0,
        }

        return render_template(
            "inventory.html",
            inventory=[],
            total_value=0,
            warehouses=[],
            districts=[],
            pagination=default_pagination,
            filters=default_filters,
        )


@app.route("/payments")
def payments():
    """Payment management page"""
    try:
        logger.info("💳 Payments page accessed")

        # Get filter parameters
        warehouse_id = request.args.get("warehouse_id", type=int)
        district_id = request.args.get("district_id", type=int)
        customer_id = request.args.get("customer_id", type=int)
        limit = request.args.get("limit", 50, type=int)
        page = request.args.get("page", 1, type=int)

        # Calculate offset
        offset = (page - 1) * limit

        logger.info(
            f"   Filters: warehouse_id={warehouse_id}, district_id={district_id}, "
            f"customer_id={customer_id}, limit={limit}, page={page}"
        )

        # Get payment history with pagination
        logger.info("   Fetching payment history...")
        payments_result = payment_service.get_payment_history_paginated(
            warehouse_id=warehouse_id,
            district_id=district_id,
            customer_id=customer_id,
            limit=limit,
            offset=offset,
        )
        logger.info(
            f"   ✅ Retrieved {len(payments_result.get('payments', []))} payment records "
            f"out of {payments_result.get('total_count', 0)} total"
        )

        # Get warehouses for filter dropdown
        logger.info("   Fetching warehouses for dropdown...")
        warehouses = analytics_service.get_warehouses()
        logger.info(f"   ✅ Retrieved {len(warehouses)} warehouses")

        logger.info("   Fetching districts for dropdown...")
        districts = analytics_service.get_districts()
        logger.info(f"   ✅ Retrieved {len(districts)} districts")

        # Calculate pagination info
        total_count = payments_result.get("total_count", 0)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1

        pagination = {
            "page": page,
            "limit": limit,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_prev": payments_result.get("has_prev", False),
            "has_next": payments_result.get("has_next", False),
            "prev_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if page < total_pages else None,
            "start_item": offset + 1 if total_count > 0 else 0,
            "end_item": min(offset + limit, total_count),
        }

        return render_template(
            "payments.html",
            payments=payments_result.get("payments", []),
            warehouses=warehouses,
            districts=districts,
            pagination=pagination,
            filters={
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "limit": limit,
            },
        )

    except Exception as e:
        logger.error(f"❌ Payments page error: {str(e)}")
        flash(f"Error loading payments: {str(e)}", "error")

        # Return empty values to template on error
        return render_template(
            "payments.html",
            payments=[],
            warehouses=[],
            districts=[],
            pagination={
                "page": 1,
                "limit": 50,
                "total_count": 0,
                "total_pages": 1,
                "has_prev": False,
                "has_next": False,
                "prev_page": None,
                "next_page": None,
                "start_item": 0,
                "end_item": 0,
            },
            filters={
                "warehouse_id": None,
                "district_id": None,
                "customer_id": None,
                "limit": 50,
            },
        )


@app.route("/api/order-status/<int:warehouse_id>/<int:district_id>/<int:customer_id>")
def api_order_status(warehouse_id: int, district_id: int, customer_id: int):
    """Get order status (TPC-C Order Status Transaction)"""
    try:
        result = order_service.get_order_status(
            warehouse_id=warehouse_id, district_id=district_id, customer_id=customer_id
        )

        return jsonify(result)

    except Exception as e:
        logger.error(f"Order status API error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/delivery", methods=["POST"])
def delivery():
    data = request.get_json()
    warehouse_id = data.get("warehouse_id")
    carrier_id = data.get("carrier_id")

    try:
        delivered_orders = order_service.execute_delivery(warehouse_id, carrier_id)
        return jsonify({
            "success": True,
            "message": "Delivery completed",
            "warehouse_id": warehouse_id,
            "carrier_id": carrier_id,
            "delivered_orders": delivered_orders,  
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        app.logger.error(f"Delivery failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# API Endpoints for AJAX operations
@app.route('/api/customer_payment_summary')
def api_customer_payment_summary():
    warehouse_id = int(request.args['warehouse_id'])
    district_id = int(request.args['district_id'])
    customer_id = int(request.args['customer_id'])

    result = payment_service.get_customer_payment_summary(warehouse_id, district_id, customer_id)
    return jsonify(result)


@app.route("/api/new-order", methods=["POST"])
def api_new_order():
    """Create a new order (TPC-C New Order Transaction)"""
    start_time = time.time()

    try:
        logger.info("🛒 TPC-C New Order Transaction API called")
        data = request.get_json()
        logger.info(f"   Request data: {data}")

        # Validate top-level fields
        required_fields = ["warehouse_id", "district_id", "customer_id", "items"]
        for field in required_fields:
            if field not in data:
                logger.error(f"❌ Missing required field: {field}")
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Log item list
        logger.info(f"   📦 Items: {len(data['items'])}")
        for idx, item in enumerate(data["items"], 1):
            logger.info(f"     Item {idx}: {item}")

        # Call service
        result = order_service.execute_new_order(
            warehouse_id=data["warehouse_id"],
            district_id=data["district_id"],
            customer_id=data["customer_id"],
            items=data["items"],
        )

        execution_time = (time.time() - start_time) * 1000
        logger.info(f"   ✅ New Order Transaction completed in {execution_time:.2f}ms")
        logger.info(f"   Result: {result}")

        return jsonify(result)

    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        logger.error(
            f"   ❌ New order API error after {execution_time:.2f}ms: {str(e)}"
        )
        return jsonify({"error": str(e)}), 500


@app.route("/api/payment", methods=["POST"])
def api_payment():
    """Process a payment (TPC-C Payment Transaction)"""
    start_time = time.time()

    try:
        logger.info("💳 TPC-C Payment Transaction API called")
        data = request.get_json()
        logger.info(f"   Request data: {data}")

        # Validate required fields
        required_fields = ["warehouse_id", "district_id", "customer_id", "amount"]
        for field in required_fields:
            if field not in data:
                logger.error(f"   ❌ Missing required field: {field}")
                return jsonify({"isSuccess": False, "error": f"Missing required field: {field}"}), 400

        logger.info(
            f"   Parameters: warehouse_id={data['warehouse_id']}, district_id={data['district_id']}, "
            f"customer_id={data['customer_id']}, amount=${data['amount']:.2f}"
        )

        # Execute payment transaction
        logger.info("   🔄 Starting Payment Transaction...")
        result = payment_service.execute_payment(
            warehouse_id=data["warehouse_id"],
            district_id=data["district_id"],
            customer_id=data["customer_id"],
            amount=data["amount"],
        )

        # Convert result to JSON-compatible dict
        json_result = {
            "isSuccess": True,
            "transaction_id": result["h_id"],
            "customer_name": f"Customer {result['h_c_id']}",  # Replace if you have actual name
            "amount": float(result["h_amount"]),
            "new_balance": 1000.00,  # Replace with actual balance calculation if available
            "timestamp": result["h_date"].isoformat(),
            "data": result["h_data"]
        }

        execution_time = (time.time() - start_time) * 1000
        logger.info(f"   ✅ Payment Transaction completed in {execution_time:.2f}ms")
        logger.info(f"   Result: {json_result}")

        return jsonify(json_result)

    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        logger.error(f"   ❌ Payment API error after {execution_time:.2f}ms: {str(e)}")
        return jsonify({"isSuccess": False, "error": str(e)}), 500


@app.route("/api/stock-level/<int:warehouse_id>/<int:district_id>")
def api_stock_level(warehouse_id: int, district_id: int):
    try:
        threshold = request.args.get("threshold", 10, type=int)
  
        low_stock_items = inventory_service.get_low_stock_items(
            warehouse_id=warehouse_id,
            district_id=district_id,
            threshold=threshold
        )

        response = {
            "success": True,
            "warehouse_id": warehouse_id,
            "district_id": district_id,
            "threshold": threshold,
            "low_stock_count": len(low_stock_items),
            "low_stock_items": low_stock_items,
        }
        return jsonify(response)

    except Exception as e:
        logger.error(f"Stock level API error: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500




# Testing and Validation Endpoints


@app.route("/test/acid")
def test_acid():
    """ACID compliance testing page"""
    try:
        return render_template(
            "test_acid.html", provider=db_connector.get_provider_name()
        )
    except Exception as e:
        logger.error(f"ACID test page error: {str(e)}")
        flash(f"Error loading ACID test page: {str(e)}", "error")
        return redirect(url_for("dashboard"))


@app.route("/test/multi-region")
def test_multi_region():
    """Multi-region testing page"""
    try:
        logger.info("🌍 Multi-region test page accessed")

        # Get current region information
        current_region = os.environ.get("REGION_NAME")
        provider_name = db_connector.get_provider_name()

        logger.info(f"   Current Region: {current_region}")
        logger.info(f"   Provider: {provider_name}")

        return render_template(
            "test_multi_region.html",
            provider=provider_name,
            current_region=current_region,
        )
    except Exception as e:
        logger.error(f"Multi-region test page error: {str(e)}")
        flash(f"Error loading multi-region test page: {str(e)}", "error")
        return redirect(url_for("dashboard"))


@app.route("/api/test/acid/<test_type>", methods=["POST"])
def api_test_acid(test_type: str):
    """Execute ACID compliance tests"""
    try:
        
        from tests.acid_tests import ACIDTests
        
        # Initialize ACID tests
        from tests.acid_tests import ACIDTests
        acid_tests = ACIDTests(db_connector)
        logger.info(f"✅ ACID tests initialized for {acid_tests.provider_name}")

        if test_type == "atomicity":
            result = acid_tests.test_atomicity()
        elif test_type == "consistency":
            result = acid_tests.test_consistency()
        elif test_type == "isolation":
            result = acid_tests.test_isolation()
        elif test_type == "durability":
            result = acid_tests.test_durability()
        elif test_type == "all":
            result = acid_tests.run_all_tests()
        else:
            return jsonify({"error": f"Unknown test type: {test_type}"}), 400

        return jsonify(result)

    except Exception as e:
        logger.error(f"ACID test API error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/test/multi-region/create-order", methods=["POST"])
def api_test_multi_region_create_order():
    """Create an order with region tracking for multi-region testing"""
    import time

    start_time = time.time()

    try:
        logger.info("🌍 Multi-region Create Order API called")
        data = request.get_json()
        logger.info(f"   Request data: {data}")

        # Validate required fields
        required_fields = ["warehouse_id", "district_id", "customer_id", "items"]
        for field in required_fields:
            if field not in data:
                logger.error(f"   ❌ Missing required field: {field}")
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Get current region
        current_region = os.environ.get("REGION_NAME")

        logger.info(
            f"   Parameters: warehouse_id={data['warehouse_id']}, district_id={data['district_id']}, customer_id={data['customer_id']}, items_count={len(data['items'])}, region={current_region}"
        )

        # Execute new order transaction with region tracking
        logger.info("   🔄 Starting Multi-Region New Order Transaction...")
        result = order_service.execute_new_order(
            warehouse_id=data["warehouse_id"],
            district_id=data["district_id"],
            customer_id=data["customer_id"],
            items=data["items"],
        )

        execution_time = (time.time() - start_time) * 1000
        logger.info(
            f"   ✅ Multi-Region New Order Transaction completed in {execution_time:.2f}ms"
        )
        logger.info(f"   Result: {result}")

        # Add execution metadata
        if result.get("success"):
            result["execution_time_ms"] = round(execution_time, 2)
            result["executed_in_region"] = current_region
            result["provider"] = db_connector.get_provider_name()

        return jsonify(result)

    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        logger.error(
            f"   ❌ Multi-region create order API error after {execution_time:.2f}ms: {str(e)}"
        )
        return jsonify(
            {"error": str(e), "execution_time_ms": round(execution_time, 2)}
        ), 500

# @app.route("/api/test/multi-region/orders-by-region")
# def api_test_multi_region_orders_by_region():
    # """Get orders grouped by region for multi-region testing (metadata only, no DB column)"""
    # try:
    #     logger.info("🌍 Multi-region Orders by Region API called")

    #     # Query without region_created column
    #     query = """
    #         SELECT 
    #             COUNT(*) as order_count,
    #             MIN(o_entry_d) as first_order,
    #             MAX(o_entry_d) as last_order
    #         FROM orders
    #     """

    #     results = db_connector.execute_query(query)
    #     row = results[0] if results else None

    #     # Get region metadata from ENV
    #     current_region = os.environ.get("REGION_NAME", "default")

    #     region_stats = []
    #     if row:
    #         region_stats.append(
    #             {
    #                 "region": current_region,
    #                 "order_count": row["order_count"],
    #                 "first_order": row["first_order"].isoformat() if row["first_order"] else None,
    #                 "last_order": row["last_order"].isoformat() if row["last_order"] else None,
    #             }
    #         )

    #     logger.info(
    #         f"   ✅ Retrieved region statistics for {len(region_stats)} region(s)"
    #     )

    #     return jsonify(
    #         {
    #             "success": True,
    #             "region_stats": region_stats,
    #             "current_region": current_region,
    #             "provider": db_connector.get_provider_name(),
    #         }
    #     )

    # except Exception as e:
    #     logger.error(f"Multi-region orders by region API error: {str(e)}")
    #     return jsonify({"error": str(e)}), 500


@app.route("/api/test/multi-region/orders-by-region")
def api_test_multi_region_orders_by_region():
    """Get orders grouped by region for multi-region testing"""
    try:
        logger.info("🌍 Multi-region Orders by Region API called")

        # Get orders with region information
        query = """
            SELECT 
                COALESCE(region_created, 'unknown') as region,
                COUNT(*) as order_count,
                MIN(o_entry_d) as first_order,
                MAX(o_entry_d) as last_order
            FROM orders 
            GROUP BY COALESCE(region_created, 'unknown')
            ORDER BY order_count DESC
        """

        results = db_connector.execute_query(query)

        # Format results
        region_stats = []
        for row in results:
            region_stats.append(
                {
                    "region": row["region"],
                    "order_count": row["order_count"],
                    "first_order": row["first_order"].isoformat()
                    if row["first_order"]
                    else None,
                    "last_order": row["last_order"].isoformat()
                    if row["last_order"]
                    else None,
                }
            )

        logger.info(
            f"   ✅ Retrieved region statistics for {len(region_stats)} regions"
        )

        return jsonify(
            {
                "success": True,
                "region_stats": region_stats,
                "current_region": os.environ.get("REGION_NAME"),
                "provider": db_connector.get_provider_name(),
            }
        )

    except Exception as e:
        logger.error(f"Multi-region orders by region API error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/test/multi-region/recent-orders")
def api_test_multi_region_recent_orders():
    """Get recent orders with region information for multi-region testing"""
    try:
        logger.info("🌍 Multi-region Recent Orders API called")

        limit = request.args.get("limit", 20, type=int)

        # Get recent orders with region information
        # Note: Changed 'no' alias to 'new_ord' to avoid Spanner reserved keyword conflict
        query = """
            SELECT 
                o.o_id,
                o.o_w_id,
                o.o_d_id,
                o.o_c_id,
                o.o_entry_d,
                COALESCE(o.region_created, 'unknown') as region_created,
                c.c_first,
                c.c_middle,
                c.c_last,
                CASE WHEN new_ord.no_o_id IS NOT NULL THEN 'New' ELSE 'Delivered' END as status
            FROM orders o
            JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
            LEFT JOIN new_order new_ord ON new_ord.no_w_id = o.o_w_id AND new_ord.no_d_id = o.o_d_id AND new_ord.no_o_id = o.o_id
            ORDER BY o.o_entry_d DESC
            LIMIT %s
        """

        results = db_connector.execute_query(query, (limit,))

        # Format results
        orders = []
        for row in results:
            orders.append(
                {
                    "order_id": row["o_id"],
                    "warehouse_id": row["o_w_id"],
                    "district_id": row["o_d_id"],
                    "customer_id": row["o_c_id"],
                    "order_date": row["o_entry_d"].isoformat()
                    if row["o_entry_d"]
                    else None,
                    "region_created": row["region_created"],
                    "customer_name": f"{row['c_first']} {row['c_middle']} {row['c_last']}",
                    "status": row["status"],
                }
            )

        logger.info(
            f"   ✅ Retrieved {len(orders)} recent orders with region information"
        )

        return jsonify(
            {
                "success": True,
                "orders": orders,
                "provider": db_connector.get_provider_name(),
            }
        )

    except Exception as e:
        logger.error(f"Multi-region recent orders API error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/dashboard-metrics")
def dashboard_metrics():
    try:
        metrics = analytics_service.get_dashboard_metrics()
        return jsonify({"metrics": metrics})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@app.route("/api/health")
def api_health():
    """Health check endpoint"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "provider": db_connector.get_provider_name(),
            "database_connection": db_connector.test_connection(),
        }

        return jsonify(health_status)

    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return jsonify(
            {
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
            }
        ), 500


# Error handlers


@app.errorhandler(404)
def not_found_error(error):
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return render_template("500.html"), 500


if __name__ == "__main__":
    # Development server
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") == "development"

    app.run(host="0.0.0.0", port=port, debug=debug)
