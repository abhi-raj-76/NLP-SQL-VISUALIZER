import unittest
import pandas as pd
from src.database_connection import DatabaseConnector


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db_connector = DatabaseConnector()

    def test_database_connection(self):
        self.assertIsNotNone(self.db_connector.engine)

    def test_load_excel(self):
        # This would test with a sample Excel file
        pass


if __name__ == '__main__':
    unittest.main()