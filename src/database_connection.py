import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, inspect, text
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self):
        self.engine = create_engine('sqlite:///background_checks.db')
        self.metadata = MetaData()

    def clear_database(self):
        """Drop all existing tables to prevent duplicates"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            with self.engine.connect() as conn:
                for table in tables:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table}"'))
                    logger.info(f"Dropped table: {table}")
            logger.info("Database cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing database: {str(e)}")

    def load_excel_to_sql(self, file_path):
        """Load all Excel sheets into SQL database, skipping non-data sheets"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Excel file not found: {file_path}")

            # Clear existing tables to avoid duplicates
            self.clear_database()

            excel_file = pd.ExcelFile(file_path)
            # Skip non-data sheets
            skip_sheets = ['Readme', 'readme']

            # Valid data tables
            valid_tables = [
                'Company Table', 'Order_Request Table', 'Package Table',
                'Search Table', 'Search_Type Table', 'Subject Table', 'Search_status'
            ]

            # Load each sheet only once
            loaded_tables = []
            for sheet_name in excel_file.sheet_names:
                if sheet_name.lower() in [s.lower() for s in skip_sheets]:
                    logger.info(f"Skipping non-data sheet: {sheet_name}")
                    continue

                if sheet_name not in valid_tables:
                    logger.warning(f"Sheet {sheet_name} not in expected tables, skipping")
                    continue

                if sheet_name in loaded_tables:
                    logger.warning(f"Sheet {sheet_name} already loaded, skipping duplicate")
                    continue

                try:
                    df = excel_file.parse(sheet_name)
                    # Clean column names
                    df.columns = [col.strip().lower().replace(' ', '_').replace('(', '').replace(')', '')
                                  for col in df.columns if col]
                    # Replace 'NULL', 'NONE', 'NA' with np.nan
                    df = df.replace(['NULL', 'NONE', 'NA'], np.nan)
                    # Use the EXACT sheet name as table name
                    table_name = sheet_name
                    df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                    loaded_tables.append(table_name)
                    logger.info(f"Loaded table: {table_name}")
                except Exception as e:
                    logger.error(f"Error loading sheet {sheet_name}: {str(e)}")
                    continue

            # Validate status codes for Search Table after all sheets are loaded
            if 'Search Table' in loaded_tables and 'Search_status' in loaded_tables:
                try:
                    df_search = pd.read_sql('SELECT search_status FROM "Search Table"', self.engine)
                    valid_codes = pd.read_sql('SELECT status_code FROM "Search_status"', self.engine)['status_code'].tolist()
                    invalid_codes = df_search['search_status'].dropna().unique()
                    invalid_codes = [code for code in invalid_codes if code not in valid_codes]
                    if invalid_codes:
                        logger.warning(f"Invalid status codes found in Search Table: {invalid_codes}. Consider adding to Search_status table with appropriate status descriptions.")
                except Exception as e:
                    logger.error(f"Error validating status codes: {str(e)}")
            else:
                logger.warning("Search Table or Search_status not found, skipping status code validation")

            # Create indexes for performance
            with self.engine.connect() as conn:
                try:
                    conn.execute(text('CREATE INDEX IF NOT EXISTS idx_search_subject ON "Search Table" (subject_id)'))
                    conn.execute(text('CREATE INDEX IF NOT EXISTS idx_order_subject ON "Order_Request Table" (order_subjectid)'))
                    conn.execute(text('CREATE INDEX IF NOT EXISTS idx_search_status ON "Search Table" (search_status)'))
                    logger.info("Indexes created successfully")
                except Exception as e:
                    logger.error(f"Error creating indexes: {str(e)}")

            if loaded_tables:
                logger.info(f"Database loaded successfully with tables: {loaded_tables}")
                return True
            else:
                logger.error("No tables were loaded successfully")
                return False

        except Exception as e:
            logger.error(f"Error loading database: {str(e)}")
            return False

    def check_table_exists(self, table_name):
        """Check if a table exists in the database"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

# Initialize database connector
db_connector = DatabaseConnector()