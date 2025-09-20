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
        """Initialize database connector with SQLite engine"""
        try:
            self.engine = create_engine('sqlite:///background_checks.db', echo=False)
            self.metadata = MetaData()
            logger.info("Database engine created successfully")
        except Exception as e:
            logger.error(f"Failed to create database engine: {str(e)}")
            raise

    def clear_database(self):
        """Drop all existing tables to prevent duplicates"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            with self.engine.connect() as conn:
                for table in tables:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table}"'))
                    logger.info(f"Dropped table: {table}")
                conn.commit()
            logger.info("Database cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing database: {str(e)}")

    def validate_excel_file(self, file_path):
        """Validate Excel file before processing"""
        if not os.path.exists(file_path):
            logger.error(f"Excel file not found: {file_path}")
            return False

        if not os.access(file_path, os.R_OK):
            logger.error(f"Cannot read Excel file: {file_path}")
            return False

        try:
            # Try to open Excel file
            pd.ExcelFile(file_path)
            return True
        except Exception as e:
            logger.error(f"Invalid Excel file: {str(e)}")
            return False

    def load_excel_to_sql(self, file_path):
        """Load all Excel sheets into SQL database with enhanced error handling"""
        try:
            # Validate file first
            if not self.validate_excel_file(file_path):
                return False

            # Clear existing database
            self.clear_database()

            # Load Excel file
            excel_file = pd.ExcelFile(file_path)

            # Expected sheet names based on your data structure
            expected_sheets = [
                'Search_status', 'Search Table', 'Search_Type Table',
                'Subject Table', 'Company Table', 'Package Table',
                'Order_Request Table'
            ]

            # Skip non-data sheets
            skip_sheets = ['Readme', 'readme']

            loaded_count = 0
            failed_sheets = []

            for sheet_name in excel_file.sheet_names:
                # Skip non-data sheets
                if sheet_name.lower() in [s.lower() for s in skip_sheets]:
                    logger.info(f"Skipping non-data sheet: {sheet_name}")
                    continue

                try:
                    # Load sheet data
                    df = excel_file.parse(sheet_name)

                    # Check if sheet is empty
                    if df.empty:
                        logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                        continue

                    # Clean column names - keep original names but clean whitespace
                    df.columns = [str(col).strip() if pd.notna(col) else f'col_{i}'
                                  for i, col in enumerate(df.columns)]

                    # Remove completely empty rows
                    df = df.dropna(how='all')

                    # Replace various null representations with actual NaN
                    null_values = ['NULL', 'NONE', 'NA', '', 'null', 'none', 'na']
                    df = df.replace(null_values, pd.NA)

                    # Save to database using exact sheet name as table name
                    df.to_sql(sheet_name, self.engine, if_exists='replace', index=False)
                    loaded_count += 1
                    logger.info(f"Successfully loaded '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")

                except Exception as e:
                    logger.error(f"Error loading sheet '{sheet_name}': {str(e)}")
                    failed_sheets.append(sheet_name)
                    continue

            # Create indexes for better performance
            self._create_indexes()

            # Log summary
            if loaded_count > 0:
                logger.info(f"Database loading completed: {loaded_count} tables loaded successfully")
                if failed_sheets:
                    logger.warning(f"Failed to load sheets: {failed_sheets}")
                return True
            else:
                logger.error("No tables were loaded successfully")
                return False

        except Exception as e:
            logger.error(f"Critical error loading database: {str(e)}")
            return False

    def _create_indexes(self):
        """Create indexes for better query performance"""
        try:
            with self.engine.connect() as conn:
                # Create indexes based on common query patterns
                indexes = [
                    ('idx_search_subject', 'Search Table', 'subject_id'),
                    ('idx_search_status', 'Search Table', 'search_status'),
                    ('idx_search_type', 'Search Table', 'search_type_code'),
                    ('idx_order_subject', 'Order_Request Table', 'order_subjectid'),
                    ('idx_order_company', 'Order_Request Table', 'order_companycode'),
                ]

                for index_name, table_name, column_name in indexes:
                    try:
                        # Check if table and column exist before creating index
                        inspector = inspect(self.engine)
                        if table_name in inspector.get_table_names():
                            columns = [col['name'] for col in inspector.get_columns(table_name)]
                            if column_name in columns:
                                conn.execute(
                                    text(f'CREATE INDEX IF NOT EXISTS {index_name} ON "{table_name}" ({column_name})'))
                                logger.info(f"Created index: {index_name}")
                    except Exception as e:
                        logger.warning(f"Could not create index {index_name}: {str(e)}")

                conn.commit()
                logger.info("Index creation completed")
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")

    def get_table_info(self):
        """Get information about all tables in the database"""
        try:
            inspector = inspect(self.engine)
            table_info = {}

            for table_name in inspector.get_table_names():
                columns = inspector.get_columns(table_name)
                table_info[table_name] = {
                    'columns': [col['name'] for col in columns],
                    'column_types': {col['name']: str(col['type']) for col in columns}
                }

                # Get row count
                with self.engine.connect() as conn:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                    table_info[table_name]['row_count'] = result.fetchone()[0]

            return table_info
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return {}

    def check_table_exists(self, table_name):
        """Check if a table exists in the database"""
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            return False

    def test_query(self, table_name, limit=5):
        """Test query on a specific table"""
        try:
            if not self.check_table_exists(table_name):
                logger.warning(f"Table '{table_name}' does not exist")
                return None

            with self.engine.connect() as conn:
                query = text(f'SELECT * FROM "{table_name}" LIMIT :limit')
                result = conn.execute(query, {"limit": limit})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            logger.error(f"Test query failed for table '{table_name}': {str(e)}")
            return None


# Initialize database connector
db_connector = DatabaseConnector()