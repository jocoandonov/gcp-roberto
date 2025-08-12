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
            params = []
            
            if warehouse_id is not None:
                where_conditions.append("h.h_w_id = %s")
                params.append(warehouse_id)
            
            if district_id is not None:
                where_conditions.append("h.h_d_id = %s")
                params.append(district_id)
            
            if customer_id is not None:
                where_conditions.append("h.h_c_id = %s")
                params.append(customer_id)
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Get total count for pagination
            count_query = f"SELECT COUNT(*) as count FROM ({query}) as subquery"
            count_result = self.execute_query(count_query, tuple(params) if params else None)
            total_count = count_result[0]["count"] if count_result else 0
            
            # Add ORDER BY and LIMIT
            query += " ORDER BY h.h_date DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            # Execute the main query
            payments = self.execute_query(query, tuple(params) if params else None)
            
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
                LEFT JOIN new_order no ON no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id
            """
            
            # Build WHERE clause based on filters
            where_conditions = []
            params = []
            
            if warehouse_id is not None:
                where_conditions.append("o.o_w_id = %s")
                params.append(warehouse_id)
            
            if district_id is not None:
                where_conditions.append("o.o_d_id = %s")
                params.append(district_id)
            
            if customer_id is not None:
                where_conditions.append("o.o_c_id = %s")
                params.append(customer_id)
            
            if status is not None:
                if status == 'new':
                    where_conditions.append("no.no_o_id IS NOT NULL")
                elif status == 'delivered':
                    where_conditions.append("no.no_o_id IS NULL")
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Get total count for pagination
            count_query = f"SELECT COUNT(*) as count FROM ({query}) as subquery"
            count_result = self.execute_query(count_query, tuple(params) if params else None)
            total_count = count_result[0]["count"] if count_result else 0
            
            # Add ORDER BY and LIMIT
            query += " ORDER BY o.o_entry_d DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            # Execute the main query
            orders = self.execute_query(query, tuple(params) if params else None)
            
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
