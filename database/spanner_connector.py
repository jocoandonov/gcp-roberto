"""
Google Spanner Database Connector - WORKING IMPLEMENTATION
Fully functional connector for Google Cloud Spanner
"""

import logging
import os
from typing import Any, Dict, List, Optional

from google.cloud import spanner
from google.cloud.spanner_v1 import Client
from .base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class SpannerConnector(BaseDatabaseConnector):
    """
    Google Spanner database connector for TPC-C application
    Fully implemented with working connection and query execution
    """

    def __init__(self):
        """Initialize Google Spanner connection"""
        super().__init__()
        self.provider_name = "Google Cloud Spanner"

        # Read configuration from environment
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.instance_id = os.getenv("SPANNER_INSTANCE_ID")
        self.database_id = os.getenv("SPANNER_DATABASE_ID")
        self.credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Display configuration status
        print(f"🔧 Spanner Configuration:")
        print(f"   Project ID: {self.project_id or '❌ NOT SET'}")
        print(f"   Instance ID: {self.instance_id or '❌ NOT SET'}")
        print(f"   Database ID: {self.database_id or '❌ NOT SET'}")
        
        if self.credentials_path:
            print(f"   Credentials: ✅ {self.credentials_path}")
        else:
            print(f"   Credentials: ❌ NOT SET")

        # Initialize Spanner client and database connections
        self.client = None
        self.instance = None
        self.database = None
        
        try:
            self._initialize_spanner_client()
        except Exception as e:
            logger.error(f"Failed to initialize Spanner client: {str(e)}")
            print(f"❌ Failed to initialize Spanner client: {str(e)}")

    def _initialize_spanner_client(self):
        """Initialize Spanner client and database connections"""
        if not all([self.project_id, self.instance_id, self.database_id]):
            raise ValueError("Missing required Spanner configuration")
        
        try:
            # Create Spanner client
            self.client = spanner.Client(project=self.project_id)
            print(f"✅ Spanner client created for project: {self.project_id}")
            
            # Get instance and database
            self.instance = self.client.instance(self.instance_id)
            self.database = self.instance.database(self.database_id)
            print(f"✅ Connected to instance: {self.instance_id}")
            print(f"✅ Connected to database: {self.database_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spanner connections: {str(e)}")
            print(f"❌ Failed to initialize Spanner connections: {str(e)}")
            raise

    def test_connection(self) -> bool:
        """Test connection to Google Spanner database"""
        try:
            if not self.database:
                print("❌ No database connection available")
                return False
            
            # Execute a simple test query using snapshot directly
            with self.database.snapshot() as snapshot:
                print("🔍 Executing test query: SELECT 1 as test")
                results = snapshot.execute_sql("SELECT 1 as test")
                
                print(f"🔍 Results type: {type(results)}")
                print(f"🔍 Results: {results}")
                
                if results:
                    print("✅ Spanner connection test successful")
                    
                    # Now test warehouse table count
                    print("🔍 Testing warehouse table access...")
                    try:
                        warehouse_results = snapshot.execute_sql("SELECT COUNT(*) as warehouse_count FROM warehouse")
                        if warehouse_results:
                            for row in warehouse_results:
                                warehouse_count = row[0] if row else 0
                                print(f"📊 Warehouse table count: {warehouse_count}")
                        else:
                            print("❌ Warehouse query returned no results")
                    except Exception as warehouse_e:
                        print(f"❌ Warehouse table query failed: {str(warehouse_e)}")
                    
                    return True
                else:
                    print("❌ Basic test query failed")
                    return False
                
        except Exception as e:
            logger.error(f"Spanner connection test failed: {str(e)}")
            print(f"❌ Spanner connection test failed: {str(e)}")
            return False

    def execute_ddl(self, ddl_statement: str) -> bool:
        """
        Execute DDL statements (CREATE, DROP, ALTER, etc.)
        
        Args:
            ddl_statement: The DDL SQL statement to execute
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"🔧 Executing DDL: {ddl_statement[:100]}...")
            
            # For Spanner, DDL operations must go through update_ddl(), not execute_sql()
            # This requires admin privileges and should be used carefully
            
            if not self.database:
                logger.error("❌ No database connection available for DDL operations")
                return False
            
            # Execute DDL through the proper Spanner method
            operation = self.database.update_ddl([ddl_statement])
            
            # Wait for the operation to complete
            operation.result()
            
            logger.info("✅ DDL executed successfully")
            return True
                    
        except Exception as e:
            logger.error(f"❌ DDL execution failed: {str(e)}")
            return False

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """Execute SQL query on Google Spanner"""
        try:
            if not self.database:
                logger.error("No database connection available")
                print("❌ No database connection available")
                return []
            
            # Execute the query
            with self.database.snapshot() as snapshot:
                if params:
                    results_iter = snapshot.execute_sql(query, params=params)
                else:
                    results_iter = snapshot.execute_sql(query)
                
                # Convert iterator to list so we can safely inspect and re-use it
                rows_data = list(results_iter)
                
                # Column names - use Spanner's fields metadata when available
                if hasattr(results_iter, 'fields') and results_iter.fields:
                    column_names = [field.name for field in results_iter.fields]
                else:
                    # Fallback: try to extract column names from the query
                    query_upper = query.upper()
                    if 'SELECT' in query_upper:
                        # Extract column names from SELECT clause
                        select_start = query_upper.find('SELECT') + 6
                        from_start = query_upper.find('FROM')
                        if from_start > select_start:
                            select_clause = query[select_start:from_start].strip()
                            # Split by comma and extract column names
                            for col in select_clause.split(','):
                                col = col.strip()
                                # Handle "column AS alias" syntax
                                if ' AS ' in col.upper():
                                    col = col.split(' AS ')[1].strip()
                                # Remove table prefixes like "table.column"
                                if '.' in col:
                                    col = col.split('.')[-1].strip()
                                column_names.append(col)
                
                # If we still don't have column names, use generic ones
                if not column_names:
                    # For COUNT(*) queries, use 'count' as the column name
                    if 'COUNT(*)' in query.upper():
                        column_names = ['count']
                    else:
                        # Use the first row to determine column count
                        column_names = [f"col_{i}" for i in range(len(rows_data[0]) if rows_data else 0)]
                
                # Build dict rows
                rows = []
                for row in rows_data:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                        if hasattr(value, 'isoformat'):  # datetime
                            row_dict[col_name] = value.isoformat()
                        elif value is None:
                            row_dict[col_name] = None
                        else:
                            row_dict[col_name] = value
                    rows.append(row_dict)
                
                return rows
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            print(f"❌ Query execution failed: {str(e)}")
            print(f"   Query: {query}")
            return []

    def get_provider_name(self) -> str:
        """Get the database provider name"""
        return self.provider_name

    def _convert_query_to_spanner_format(self, query: str, param_values: List[Any]) -> tuple[str, Dict[str, Any], Dict[str, Any]]:
        """
        Convert a query with %s placeholders to Spanner PostgreSQL format with $1, $2, $3... placeholders
        
        Args:
            query: SQL query with %s placeholders
            param_values: List of parameter values
            
        Returns:
            tuple: (converted_query, params_dict, param_types_dict)
        """
        converted_query = query
        params = {}
        param_types = {}
        
        for i, value in enumerate(param_values, 1):
            placeholder = f"${i}"
            converted_query = converted_query.replace("%s", placeholder, 1)
            params[f"p{i}"] = value
            
            # Determine parameter type
            if isinstance(value, int):
                param_types[f"p{i}"] = spanner.param_types.INT64
            elif isinstance(value, float):
                param_types[f"p{i}"] = spanner.param_types.FLOAT64
            elif isinstance(value, str):
                param_types[f"p{i}"] = spanner.param_types.STRING
            elif isinstance(value, bool):
                param_types[f"p{i}"] = spanner.param_types.BOOL
            else:
                # Default to STRING for unknown types
                param_types[f"p{i}"] = spanner.param_types.STRING
        
        return converted_query, params, param_types

    def get_payment_history_paginated(
        self,
        warehouse_id: Optional[int] = None,
        district_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get payment history with pagination and filtering"""
        try:
            # Build the base query
            query = """
                SELECT h.h_w_id, h.h_d_id, h.h_c_id, h.h_amount, h.h_date,
                       c.c_first, c.c_middle, c.c_last,
                       w.w_name as warehouse_name, d.d_name as district_name
                FROM history h
                JOIN customer c ON c.c_w_id = h.h_w_id AND c.c_d_id = h.h_d_id AND c.c_id = h.h_c_id
                JOIN warehouse w ON w.w_id = h.h_w_id
                JOIN district d ON d.d_w_id = h.h_w_id AND d.d_id = h.h_d_id
            """
            
            # Build WHERE clause based on filters
            where_conditions = []
            params = {}
            param_types = {}
            param_counter = 1
            
            if warehouse_id is not None:
                where_conditions.append(f"h.h_w_id = ${param_counter}")
                params[f"p{param_counter}"] = warehouse_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if district_id is not None:
                where_conditions.append(f"h.h_d_id = ${param_counter}")
                params[f"p{param_counter}"] = district_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if customer_id is not None:
                where_conditions.append(f"h.h_c_id = ${param_counter}")
                params[f"p{param_counter}"] = customer_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Get total count for pagination
            count_query = f"SELECT COUNT(*) as count FROM ({query}) as subquery"
            
            # Execute count query with current params
            with self.database.snapshot() as snapshot:
                count_results = snapshot.execute_sql(count_query, params=params, param_types=param_types)
                total_count = 0
                for row in count_results:
                    total_count = int(row[0]) if row[0] is not None else 0
                    break
            
            # Add ORDER BY and LIMIT
            query += f" ORDER BY h.h_date DESC LIMIT ${param_counter} OFFSET ${param_counter + 1}"
            params[f"p{param_counter}"] = limit
            param_types[f"p{param_counter}"] = spanner.param_types.INT64
            params[f"p{param_counter + 1}"] = offset
            param_types[f"p{param_counter + 1}"] = spanner.param_types.INT64
            
            # Execute the main query
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(query, params=params, param_types=param_types)
                
                # Convert results to list of dictionaries
                payments = []
                if hasattr(results, 'fields') and results.fields:
                    column_names = [field.name for field in results.fields]
                else:
                    # Fallback column names for payment history
                    column_names = ['h_w_id', 'h_d_id', 'h_c_id', 'h_amount', 'h_date', 'c_first', 'c_middle', 'c_last', 'warehouse_name', 'district_name']
                
                for row in results:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                        if hasattr(value, 'isoformat'):  # datetime
                            row_dict[col_name] = value.isoformat()
                        elif value is None:
                            row_dict[col_name] = None
                        else:
                            row_dict[col_name] = value
                    payments.append(row_dict)
            
            # Calculate pagination info
            has_next = (offset + limit) < total_count
            has_prev = offset > 0
            
            return {
                "payments": payments,
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": has_next,
                "has_prev": has_prev,
            }
            
        except Exception as e:
            logger.error(f"Failed to get payment history paginated: {str(e)}")
            return {
                "payments": [],
                "total_count": 0,
                "limit": limit,
                "offset": offset,
                "has_next": False,
                "has_prev": False,
            }

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
            # Build the base query using order_table (not orders)
            query = """
                SELECT o.o_id, o.o_w_id, o.o_d_id, o.o_c_id, o.o_entry_d, o.o_ol_cnt, o.o_carrier_id,
                       c.c_first, c.c_middle, c.c_last,
                       CASE WHEN no.no_o_id IS NOT NULL THEN 'New' ELSE 'Delivered' END as status
                FROM order_table o
                JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
                LEFT JOIN new_order no ON no.no_w_id = o.o_w_id AND no.no_d_id =  o.o_d_id AND no.no_o_id = o.o_id
            """
            
            # Build WHERE clause based on filters
            where_conditions = []
            params = {}
            param_types = {}
            param_counter = 1
            
            if warehouse_id is not None:
                where_conditions.append(f"o.o_w_id = ${param_counter}")
                params[f"p{param_counter}"] = warehouse_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if district_id is not None:
                where_conditions.append(f"o.o_d_id = ${param_counter}")
                params[f"p{param_counter}"] = district_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if customer_id is not None:
                where_conditions.append(f"o.o_c_id = ${param_counter}")
                params[f"p{param_counter}"] = customer_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if status is not None:
                if status == 'new':
                    where_conditions.append("no.no_o_id IS NOT NULL")
                elif status == 'delivered':
                    where_conditions.append("no.no_o_id IS NULL")
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Get total count for pagination
            count_query = f"SELECT COUNT(*) as count FROM ({query}) as subquery"
            
            # Execute count query with current params
            with self.database.snapshot() as snapshot:
                count_results = snapshot.execute_sql(count_query, params=params, param_types=param_types)
                total_count = 0
                for row in count_results:
                    total_count = int(row[0]) if row[0] is not None else 0
                    break
            
            # Add ORDER BY and LIMIT
            query += f" ORDER BY o.o_entry_d DESC LIMIT ${param_counter} OFFSET ${param_counter + 1}"
            params[f"p{param_counter}"] = limit
            param_types[f"p{param_counter}"] = spanner.param_types.INT64
            params[f"p{param_counter + 1}"] = offset
            param_types[f"p{param_counter + 1}"] = spanner.param_types.INT64
            
            # Execute the main query
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(query, params=params, param_types=param_types)
                
                # Convert results to list of dictionaries
                orders = []
                if hasattr(results, 'fields') and results.fields:
                    column_names = [field.name for field in results.fields]
                else:
                    # Fallback column names for orders
                    column_names = ['o_id', 'o_w_id', 'o_d_id', 'o_c_id', 'o_entry_d', 'o_ol_cnt', 'o_carrier_id', 'c_first', 'c_middle', 'c_last', 'status']
                
                for row in results:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                        if hasattr(value, 'isoformat'):  # datetime
                            row_dict[col_name] = value.isoformat()
                        elif value is None:
                            row_dict[col_name] = None
                        else:
                            row_dict[col_name] = value
                    orders.append(row_dict)
            
            # Calculate pagination info
            has_next = (offset + limit) < total_count
            has_prev = offset > 0
            
            return {
                "orders": orders,
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": has_next,
                "has_prev": has_prev,
            }
            
        except Exception as e:
            logger.error(f"Failed to get orders: {str(e)}")
            return {
                "orders": [],
                "total_count": 0,
                "limit": limit,
                "offset": offset,
                "has_next": False,
                "has_prev": False,
            }

    def get_order_status(
        self, warehouse_id: int, district_id: int, customer_id: int
    ) -> Dict[str, Any]:
        """Get order status for a customer"""
        try:
            # Query to get order status information
            query = """
                SELECT o.o_id, o.o_w_id, o.o_d_id, o.o_c_id, o.o_entry_d, o.o_carrier_id,
                       c.c_first, c.c_middle, c.c_last, c.c_balance,
                       CASE WHEN no.no_o_id IS NOT NULL THEN 'New' ELSE 'Delivered' END as status
                FROM order_table o
                JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
                LEFT JOIN new_order no ON no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id
                WHERE o.o_w_id = $1 AND o.o_d_id = $2 AND o.o_c_id = $3
                ORDER BY o.o_entry_d DESC
                LIMIT 1
            """
            
            params = {"p1": warehouse_id, "p2": district_id, "p3": customer_id}
            param_types = {
                "p1": spanner.param_types.INT64,
                "p2": spanner.param_types.INT64,
                "p3": spanner.param_types.INT64
            }
            
            # First snapshot for order data
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(query, params=params, param_types=param_types)
                
                if not results:
                    return {"success": False, "error": "Order not found"}
                
                # Get the first (most recent) order - handle Spanner results properly
                order_row = None
                for row in results:
                    order_row = row
                    break
                
                if not order_row:
                    return {"success": False, "error": "Order not found"}
                
                # Get column names from results metadata
                if hasattr(results, 'fields') and results.fields:
                    column_names = [field.name for field in results.fields]
                else:
                    # Fallback column names if metadata not available
                    column_names = ['o_id', 'o_w_id', 'o_d_id', 'o_c_id', 'o_entry_d', 'o_carrier_id', 
                                  'c_first', 'c_middle', 'c_last', 'c_balance', 'status']
                
                # Convert row to dictionary for safer access
                order_data = {}
                for i, value in enumerate(order_row):
                    if i < len(column_names):
                        col_name = column_names[i]
                        if hasattr(value, 'isoformat'):  # datetime
                            order_data[col_name] = value.isoformat()
                        elif value is None:
                            order_data[col_name] = None
                        else:
                            order_data[col_name] = value
                
                # Extract order information using dictionary keys
                order_id = order_data.get('o_id')
                order_date = order_data.get('o_entry_d')
                carrier_id = order_data.get('o_carrier_id')
                customer_name = f"{order_data.get('c_first', '')} {order_data.get('c_middle', '')} {order_data.get('c_last', '')}".strip()
                customer_balance = order_data.get('c_balance')
                status = order_data.get('status')
                
                if not order_id:
                    return {"success": False, "error": "Invalid order data structure"}
            
            # Second snapshot for order lines (separate from the first one)
            order_lines_query = """
                SELECT ol.ol_i_id, ol.ol_quantity, ol.ol_amount, ol.ol_supply_w_id, ol.ol_delivery_d,
                       i.i_name
                FROM order_line ol
                JOIN item i ON i.i_id = ol.ol_i_id
                WHERE ol.ol_w_id = $1 AND ol.ol_d_id = $2 AND ol.ol_o_id = $3
                ORDER BY ol.ol_number
            """
            
            order_lines_params = {"p1": warehouse_id, "p2": district_id, "p3": order_id}
            order_lines_param_types = {
                "p1": spanner.param_types.INT64,
                "p2": spanner.param_types.INT64,
                "p3": spanner.param_types.INT64
            }
            
            # Use a separate snapshot for order lines query
            with self.database.snapshot() as order_lines_snapshot:
                order_lines_results = order_lines_snapshot.execute_sql(order_lines_query, params=order_lines_params, param_types=order_lines_param_types)
                
                # Convert order lines to list of dictionaries
                order_lines = []
                if hasattr(order_lines_results, 'fields') and order_lines_results.fields:
                    line_column_names = [field.name for field in order_lines_results.fields]
                else:
                    line_column_names = ['ol_i_id', 'ol_quantity', 'ol_amount', 'ol_supply_w_id', 'ol_delivery_d', 'i_name']
                
                for row in order_lines_results:
                    line_dict = {}
                    for i, value in enumerate(row):
                        col_name = line_column_names[i] if i < len(line_column_names) else f"col_{i}"
                        if hasattr(value, 'isoformat'):  # datetime
                            line_dict[col_name] = value.isoformat()
                        elif value is None:
                            line_dict[col_name] = None
                        else:
                            line_dict[col_name] = value
                    order_lines.append(line_dict)
            
            return {
                "success": True,
                "order_id": order_id,
                "order_date": order_date,
                "carrier_id": carrier_id,
                "customer_name": customer_name,
                "customer_balance": customer_balance,
                "order_line_count": len(order_lines),
                "order_lines": order_lines
            }
                
        except Exception as e:
            logger.error(f"Failed to get order status: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_inventory_paginated(
        self,
        warehouse_id: Optional[int] = None,
        low_stock_threshold: Optional[int] = None,
        item_search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get inventory data with pagination and filtering"""
        try:
            # Build the base query
            query = """
                SELECT s.s_i_id, s.s_w_id, s.s_quantity, s.s_ytd, s.s_order_cnt, s.s_remote_cnt,
                       i.i_name, i.i_price, i.i_data,
                       w.w_name
                FROM stock s
                JOIN item i ON i.i_id = s.s_i_id
                JOIN warehouse w ON w.w_id = s.s_w_id
            """
            
            # Build WHERE clause based on filters
            where_conditions = []
            params = {}
            param_types = {}
            param_counter = 1
            
            if warehouse_id is not None:
                where_conditions.append(f"s.s_w_id = ${param_counter}")
                params[f"p{param_counter}"] = warehouse_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            # Only apply low stock threshold if explicitly provided (not None)
            if low_stock_threshold is not None:
                where_conditions.append(f"s.s_quantity < ${param_counter}")
                params[f"p{param_counter}"] = low_stock_threshold
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if item_search:
                where_conditions.append(f"(LOWER(i.i_name) LIKE LOWER(${param_counter}) OR LOWER(i.i_data) LIKE LOWER(${param_counter}))")
                search_param = f"%{item_search}%"
                params[f"p{param_counter}"] = search_param
                param_types[f"p{param_counter}"] = spanner.param_types.STRING
                param_counter += 1
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Get total count for pagination
            count_query = f"SELECT COUNT(*) as count FROM ({query}) as subquery"
            
            # Execute count query with current params
            with self.database.snapshot() as snapshot:
                count_results = snapshot.execute_sql(count_query, params=params, param_types=param_types)
                total_count = 0
                for row in count_results:
                    total_count = int(row[0]) if row[0] is not None else 0
                    break
            
            # Add ORDER BY and LIMIT
            query += f" ORDER BY s.s_quantity ASC LIMIT ${param_counter} OFFSET ${param_counter + 1}"
            params[f"p{param_counter}"] = limit
            param_types[f"p{param_counter}"] = spanner.param_types.INT64
            params[f"p{param_counter + 1}"] = offset
            param_types[f"p{param_counter + 1}"] = spanner.param_types.INT64
            
            # Execute the main query
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(query, params=params, param_types=param_types)
                
                # Convert results to list of dictionaries
                inventory = []
                if hasattr(results, 'fields') and results.fields:
                    column_names = [field.name for field in results.fields]
                else:
                    # Fallback column names for inventory
                    column_names = ['s_i_id', 's_w_id', 's_quantity', 's_ytd', 's_order_cnt', 's_remote_cnt', 
                                  'i_name', 'i_price', 'i_data', 'w_name']
                
                for row in results:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                        if hasattr(value, 'isoformat'):  # datetime
                            row_dict[col_name] = value.isoformat()
                        elif value is None:
                            row_dict[col_name] = None
                        else:
                            row_dict[col_name] = value
                    inventory.append(row_dict)
            
            # Calculate pagination info
            has_next = (offset + limit) < total_count
            has_prev = offset > 0
            
            return {
                "inventory": inventory,
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": has_next,
                "has_prev": has_prev,
            }
            
        except Exception as e:
            logger.error(f"Failed to get inventory paginated: {str(e)}")
            return {
                "inventory": [],
                "total_count": 0,
                "limit": limit,
                "offset": offset,
                "has_next": False,
                "has_prev": False,
            }

    def get_inventory(
        self,
        warehouse_id: Optional[int] = None,
        low_stock_threshold: Optional[int] = None,
        item_search: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get basic inventory data with optional filters (no pagination)"""
        try:
            # Build the base query
            query = """
                SELECT s.s_i_id, s.s_w_id, s.s_quantity, s.s_ytd, s.s_order_cnt, s.s_remote_cnt,
                       i.i_name, i.i_price, i.i_data,
                       w.w_name
                FROM stock s
                JOIN item i ON i.i_id = s.s_i_id
                JOIN warehouse w ON w.w_id = s.s_w_id
            """
            
            # Build WHERE clause based on filters
            where_conditions = []
            params = {}
            param_types = {}
            param_counter = 1
            
            if warehouse_id is not None:
                where_conditions.append(f"s.s_w_id = ${param_counter}")
                params[f"p{param_counter}"] = warehouse_id
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            # Only apply low stock threshold if explicitly provided (not None)
            if low_stock_threshold is not None:
                where_conditions.append(f"s.s_quantity < ${param_counter}")
                params[f"p{param_counter}"] = low_stock_threshold
                param_types[f"p{param_counter}"] = spanner.param_types.INT64
                param_counter += 1
            
            if item_search:
                where_conditions.append(f"(LOWER(i.i_name) LIKE LOWER(${param_counter}) OR i.i_data LIKE LOWER(${param_counter}))")
                search_param = f"%{item_search}%"
                params[f"p{param_counter}"] = search_param
                param_types[f"p{param_counter}"] = spanner.param_types.STRING
                param_counter += 1
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Add ORDER BY and LIMIT
            query += f" ORDER BY s.s_quantity ASC LIMIT ${param_counter}"
            params[f"p{param_counter}"] = limit
            param_types[f"p{param_counter}"] = spanner.param_types.INT64
            
            # Execute the query
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(query, params=params, param_types=param_types)
                
                # Convert results to list of dictionaries
                inventory = []
                if hasattr(results, 'fields') and results.fields:
                    column_names = [field.name for field in results.fields]
                else:
                    # Fallback column names for inventory
                    column_names = ['s_i_id', 's_w_id', 's_quantity', 's_ytd', 's_order_cnt', 's_remote_cnt', 
                                  'i_name', 'i_price', 'i_data', 'w_name']
                
                for row in results:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                        if hasattr(value, 'isoformat'):  # datetime
                            row_dict[col_name] = value.isoformat()
                        elif value is None:
                            row_dict[col_name] = None
                        else:
                            row_dict[col_name] = value
                    inventory.append(row_dict)
                
                return inventory
                
        except Exception as e:
            logger.error(f"Failed to get inventory: {str(e)}")
            return []

    def get_stock_level(
        self, warehouse_id: int, district_id: int, threshold: int
    ) -> Dict[str, Any]:
        """Execute TPC-C Stock Level transaction"""
        try:
            # Query to get stock level information
            query = """
                SELECT COUNT(*) as low_stock_count
                FROM stock s
                JOIN order_line ol ON ol.ol_i_id = s.s_i_id 
                    AND ol.ol_w_id = s.s_w_id
                JOIN order_table o ON o.o_id = ol.ol_o_id 
                    AND o.o_w_id = ol.ol_w_id 
                    AND o.o_d_id = ol.ol_d_id
                WHERE s.s_w_id = $1 
                    AND s.s_d_id = $2 
                    AND o.o_id >= (SELECT d_next_o_id - 20 FROM district WHERE d_w_id = $1 AND d_id = $2)
                    AND o.o_id < (SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2)
                    AND s.s_quantity < $3
            """
            
            params = {"p1": warehouse_id, "p2": district_id, "p3": threshold}
            param_types = {
                "p1": spanner.param_types.INT64,
                "p2": spanner.param_types.INT64,
                "p3": spanner.param_types.INT64
            }
            
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(query, params=params, param_types=param_types)
                
                low_stock_count = 0
                for row in results:
                    low_stock_count = int(row[0]) if row[0] is not None else 0
                    break
                
                return {
                    "success": True,
                    "warehouse_id": warehouse_id,
                    "district_id": district_id,
                    "threshold": threshold,
                    "low_stock_count": low_stock_count
                }
                
        except Exception as e:
            logger.error(f"Failed to get stock level: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_table_counts(self) -> Dict[str, int]:
        """Get record counts for all major TPC-C tables"""
        table_counts = {}
        
        if not self.database:
            print("❌ No database connection available for table counts")
            return table_counts
        
        # Use the working execute_query method instead of direct Spanner calls
        tables = [
            "warehouse", "district", "customer", "order_table", 
            "order_line", "item", "stock"
        ]
        
        for table in tables:
            try:
                print(f"🔍 Testing table====================: {table}")
                # Use execute_query which handles Spanner results properly
                result = self.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                count = result[0]["count"] if result and len(result) > 0 else 0
                table_counts[table] = count
                print(f"   ✅ {table}: {count} records")
                        
            except Exception as e:
                print(f"   ❌ Error counting {table}: {str(e)}")
                table_counts[table] = 0
        
        return table_counts

    def close_connection(self):
        """Close database connection"""
        try:
            if self.client:
                self.client.close()
                print("✅ Spanner connection closed")
        except Exception as e:
            logger.error(f"Error closing Spanner connection: {str(e)}")
            print(f"❌ Error closing Spanner connection: {str(e)}")
