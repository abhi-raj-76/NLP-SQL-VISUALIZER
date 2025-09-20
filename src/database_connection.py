import pandas as pd
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.orm import sessionmaker
import os


class DatabaseConnector:
    def __init__(self):
        self.engine = create_engine('sqlite:///background_checks.db')
        self.session = sessionmaker(bind=self.engine)()
        self.metadata = MetaData()

    def load_excel_to_sql(self, file_path):
        """Load all Excel sheets into SQL database"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Excel file not found: {file_path}")

            excel_file = pd.ExcelFile(file_path)

            for sheet_name in excel_file.sheet_names:
                df = excel_file.parse(sheet_name)
                # Clean column names
                df.columns = [col.strip().lower().replace(' ', '_').replace('(', '').replace(')', '')
                              for col in df.columns if col]

                # Save to SQL
                table_name = sheet_name.lower().replace(' ', '_')
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                print(f"Loaded table: {table_name}")

            print("Database loaded successfully!")
            return True
        except Exception as e:
            print(f"Error loading database: {str(e)}")
            return False

    def check_table_exists(self, table_name):
        """Check if a table exists in the database"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()


# Initialize database connector
db_connector = DatabaseConnector()