# """
# NeonDB Database Connector - STUDY IMPLEMENTATION SKELETON
# Participants will implement this connector to integrate with NeonDB

# This file contains TODO items that participants need to complete during the study.
# """

# import logging
# import os
# from typing import Any, Dict, List, Optional

# from .base_connector import BaseDatabaseConnector

# logger = logging.getLogger(__name__)


# class NeonConnector(BaseDatabaseConnector):
#     """
#     NeonDB database connector for TPC-C application

#     Participants will implement connection management and query execution
#     for NeonDB during the UX study.
#     """

#     def __init__(self):
#         """
#         Initialize NeonDB connection

#         TODO: Implement NeonDB connection initialization
#         - Read configuration from environment variables
#         - Set up PostgreSQL connection to NeonDB
#         - Configure connection parameters and SSL settings
#         - Handle connection pooling if needed

#         Environment variables to use:
#         - NEON_CONNECTION_STRING: PostgreSQL connection string for NeonDB
#         """
#         super().__init__()
#         self.provider_name = "NeonDB"

#         # TODO: Initialize NeonDB connection
#         self.connection = None

#         # Read connection string from environment
#         self.connection_string = os.getenv("NEON_CONNECTION_STRING")

#         # TODO: Validate required configuration
#         if not self.connection_string:
#             logger.error("NEON_CONNECTION_STRING is not set.")
#         else:
#             logger.info(f"NeonConnector initialized with DB: {self.connection_string}")

#         # TODO: Initialize actual DB client if needed

#     def test_connection(self) -> bool:
#         """
#         Test connection to NeonDB database
#         """
#         try:
#             # TODO: Replace with real connection test
#             logger.info("Testing connection to NeonDB...")
#             return False  # Placeholder
#         except Exception as e:
#             logger.error(f"NeonDB connection test failed: {str(e)}")
#             return False

#     def execute_query(
#         self, query: str, params: Optional[tuple] = None
#     ) -> List[Dict[str, Any]]:
#         """
#         Execute SQL query on NeonDB
#         """
#         try:
#             logger.info(f"Executing query: {query}")
#             # TODO: Implement actual query execution
#             return []  # Placeholder
#         except Exception as e:
#             logger.error(f"NeonDB query execution failed: {str(e)}")
#             raise

#     def get_provider_name(self) -> str:
#         """Return the provider name"""
#         return self.provider_name

#     def close_connection(self):
#         """
#         Close database connections
#         """
#         try:
#             logger.info("Closing NeonDB connection...")
#             # TODO: Close any open connections
#         except Exception as e:
#             logger.error(f"Connection cleanup failed: {str(e)}")


# # ✅ This function must be outside the class
# def create_study_connector():
#     """
#     Factory function to create and return a NeonConnector instance.
#     """
#     return NeonConnector()

import logging
import os
import psycopg2
import psycopg2.extras 
from typing import Any, Dict, List, Optional
from .base_connector import BaseDatabaseConnector
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

class NeonConnector(BaseDatabaseConnector):
    def __init__(self):
        super().__init__()
        self.provider_name = "NeonDB"
        self.connection = None
        self.connection_string = os.getenv("NEON_CONNECTION_STRING")

        if not self.connection_string:
            raise ValueError("NEON_CONNECTION_STRING is not set in environment variables")

        try:
            # Establish connection
            self.connection = psycopg2.connect(self.connection_string, sslmode="require")
            self.connection.autocommit = True  # For read/write operations without manual commit
            logger.info("Connected to NeonDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to NeonDB: {str(e)}")
            raise

    def test_connection(self) -> bool:
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                logger.info("NeonDB connection test passed")
                return result[0] == 1
        except Exception as e:
            logger.error(f"NeonDB connection test failed: {str(e)}")
            return False


    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params or ())
                if cur.description:  # If the query returns rows
                    return [dict(row) for row in cur.fetchall()]
                return []
        except Exception as e:
            logger.error(f"NeonDB query execution failed: {str(e)}")
            raise

    def cursor(self, dictionary=False):
        if dictionary:
            return self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self.connection.cursor()
    
    def fetch_one(self, query, params=None):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params or {})
            return cursor.fetchone()  
        
    def fetch_all(self, query, params=None):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params or {})
            return cursor.fetchall()
        
    def get_provider_name(self) -> str:
        return self.provider_name

    def close_connection(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("NeonDB connection closed successfully")
        except Exception as e:
            logger.error(f"Connection cleanup failed: {str(e)}")
# # ✅ This function must be outside the class
def create_study_connector():
    """
    Factory function to create and return a NeonConnector instance.
    """
    return NeonConnector()
