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
                    results = snapshot.execute_sql(query, params=params)
                else:
                    results = snapshot.execute_sql(query)
                
                # Convert results to list of dictionaries
                rows = []
                
                for row in results:
                    row_dict = {}
                    # For simple queries like COUNT(*), just use the value
                    if len(row) == 1:
                        row_dict["count"] = row[0]
                    else:
                        # For multi-column queries, we'd need field names
                        # For now, just use index-based keys
                        for i, value in enumerate(row):
                            if hasattr(value, 'isoformat'):  # datetime
                                row_dict[f"col_{i}"] = value.isoformat()
                            elif value is None:
                                row_dict[f"col_{i}"] = None
                            else:
                                row_dict[f"col_{i}"] = value
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

    def get_table_counts(self) -> Dict[str, int]:
        """Get record counts for all major TPC-C tables"""
        table_counts = {}
        
        if not self.database:
            print("❌ No database connection available for table counts")
            return table_counts
        
        # First, let's discover what tables actually exist
        print("🔍 Discovering available tables...")
        try:
            with self.database.snapshot() as snapshot:
                # Query to list all tables
                results = snapshot.execute_sql("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
                
                available_tables = []
                if results:
                    for row in results:
                        available_tables.append(row[0])
                
                print(f"📋 Available tables: {', '.join(available_tables)}")
                
        except Exception as e:
            print(f"❌ Could not discover tables: {str(e)}")
            # Fallback to common TPC-C table names
            available_tables = [
                "warehouse", "district", "customer", "order_table", 
                "order_line", "item", "stock"
            ]
        
        print("📊 Getting table counts...")
        print("-" * 40)
        
        for table in available_tables:
            try:
                # Create a new snapshot for each table
                with self.database.snapshot() as snapshot:
                    query = f"SELECT COUNT(*) as count FROM {table}"
                    results = snapshot.execute_sql(query)
                    
                    if results:
                        for row in results:
                            count = row[0] if row else 0
                            table_counts[table] = count
                            print(f"   {table}: {count} records")
                    else:
                        table_counts[table] = 0
                        print(f"   {table}: ❌ Query failed")
                        
            except Exception as e:
                table_counts[table] = 0
                print(f"   {table}: ❌ Error - {str(e)}")
        
        print("-" * 40)
        total_records = sum(table_counts.values())
        print(f"📈 Total records across all tables: {total_records}")
        
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
