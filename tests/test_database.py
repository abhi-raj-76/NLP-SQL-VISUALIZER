import unittest
import pandas as pd
import tempfile
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database_connection import DatabaseConnector
from src.llm_setup import LLMQueryEngine
from src.visualizations import DataVisualizer


class TestDatabase(unittest.TestCase):
    """Test suite for database functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.db_connector = DatabaseConnector()

        # Create sample test data
        self.sample_data = {
            'Search Table': pd.DataFrame({
                'searchId': [1, 2, 3],
                'search_status': ['P', 'C', 'R'],
                'search_type_code': ['EDU', 'EMP', 'CRI'],
                'subject_id': [101, 102, 103],
                'county_name': ['County1', 'County2', 'County3'],
                'state_code': ['CA', 'NY', 'TX']
            }),
            'Subject Table': pd.DataFrame({
                'subject_id': [101, 102, 103],
                'subject_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
                'subject_contact': ['555-1234', '555-5678', '555-9012'],
                'sbj_city': ['Los Angeles', 'New York', 'Houston']
            }),
            'Search_status': pd.DataFrame({
                'Status_code': ['P', 'C', 'R', 'D'],
                'Status': ['PENDING', 'COMPLETED', 'RESOLVED', 'DRAFT']
            })
        }

    def test_database_connection(self):
        """Test database engine creation"""
        self.assertIsNotNone(self.db_connector.engine)

    def test_table_exists_check(self):
        """Test table existence checking"""
        # Initially no tables should exist
        self.assertFalse(self.db_connector.check_table_exists('nonexistent_table'))

    def test_load_sample_data(self):
        """Test loading sample data into database"""
        try:
            # Clear database first
            self.db_connector.clear_database()

            # Load sample data
            for table_name, df in self.sample_data.items():
                df.to_sql(table_name, self.db_connector.engine, if_exists='replace', index=False)

            # Verify tables exist
            for table_name in self.sample_data.keys():
                self.assertTrue(self.db_connector.check_table_exists(table_name))

            # Test query functionality
            test_df = self.db_connector.test_query('Search Table', limit=2)
            self.assertIsNotNone(test_df)
            self.assertEqual(len(test_df), 2)

        except Exception as e:
            self.fail(f"Error loading sample data: {str(e)}")

    def test_get_table_info(self):
        """Test getting table information"""
        # Load sample data first
        for table_name, df in self.sample_data.items():
            df.to_sql(table_name, self.db_connector.engine, if_exists='replace', index=False)

        table_info = self.db_connector.get_table_info()
        self.assertIsInstance(table_info, dict)
        self.assertGreater(len(table_info), 0)


class TestLLMQueryEngine(unittest.TestCase):
    """Test suite for LLM Query Engine"""

    def setUp(self):
        """Set up test fixtures"""
        self.db_connector = DatabaseConnector()

        # Load sample data
        sample_data = {
            'Search Table': pd.DataFrame({
                'searchId': [1, 2, 3, 4, 5],
                'search_status': ['P', 'C', 'R', 'P', 'C'],
                'search_type_code': ['EDU', 'EMP', 'CRI', 'EDU', 'EMP'],
                'subject_id': [101, 102, 103, 104, 105],
                'county_name': ['County1', 'County2', 'County3', 'County4', 'County5'],
                'state_code': ['CA', 'NY', 'TX', 'FL', 'WA']
            })
        }

        for table_name, df in sample_data.items():
            df.to_sql(table_name, self.db_connector.engine, if_exists='replace', index=False)

        self.query_engine = LLMQueryEngine(self.db_connector.engine)

    def test_query_engine_initialization(self):
        """Test query engine initialization"""
        self.assertIsNotNone(self.query_engine)
        self.assertIsInstance(self.query_engine.table_info, dict)

    def test_simple_query(self):
        """Test simple query processing"""
        response, sql, df = self.query_engine.query("show all background checks")

        self.assertIsNotNone(response)
        self.assertIsNotNone(sql)
        self.assertIsInstance(df, pd.DataFrame)

    def test_count_query(self):
        """Test count query processing"""
        response, sql, df = self.query_engine.query("count pending checks")

        self.assertIsNotNone(response)
        self.assertIsNotNone(sql)
        self.assertIsInstance(df, pd.DataFrame)

    def test_invalid_query(self):
        """Test handling of invalid queries"""
        response, sql, df = self.query_engine.query("DROP TABLE test")

        self.assertIn("Invalid", response)
        self.assertIsNone(sql)
        self.assertIsNone(df)


class TestDataVisualizer(unittest.TestCase):
    """Test suite for Data Visualizer"""

    def setUp(self):
        """Set up test fixtures"""
        self.db_connector = DatabaseConnector()

        # Load sample data
        sample_data = {
            'Search Table': pd.DataFrame({
                'searchId': [1, 2, 3, 4, 5],
                'search_status': ['P', 'C', 'R', 'P', 'C'],
                'search_type_code': ['EDU', 'EMP', 'CRI', 'EDU', 'EMP'],
                'state_code': ['CA', 'NY', 'TX', 'FL', 'WA']
            })
        }

        for table_name, df in sample_data.items():
            df.to_sql(table_name, self.db_connector.engine, if_exists='replace', index=False)

        self.visualizer = DataVisualizer(self.db_connector.engine)

    def test_visualizer_initialization(self):
        """Test visualizer initialization"""
        self.assertIsNotNone(self.visualizer)

    def test_status_chart_creation(self):
        """Test status pie chart creation"""
        try:
            fig = self.visualizer.create_status_pie_chart()
            # Chart creation might return None if no data, which is acceptable
            self.assertTrue(fig is None or hasattr(fig, 'data'))
        except Exception as e:
            self.fail(f"Error creating status chart: {str(e)}")

    def test_auto_visualization(self):
        """Test automatic visualization selection"""
        # Create test DataFrame
        test_df = pd.DataFrame({
            'search_status': ['P', 'C', 'R'],
            'count': [10, 15, 5]
        })

        try:
            fig = self.visualizer.auto_visualize(test_df, "Test Results")
            self.assertTrue(fig is None or hasattr(fig, 'data'))
        except Exception as e:
            self.fail(f"Error in auto visualization: {str(e)}")


class TestExcelLoading(unittest.TestCase):
    """Test suite for Excel file loading"""

    def setUp(self):
        """Set up test fixtures"""
        self.db_connector = DatabaseConnector()

    def test_file_validation(self):
        """Test Excel file validation"""
        # Test with non-existent file
        self.assertFalse(self.db_connector.validate_excel_file('nonexistent.xlsx'))

    def test_sample_excel_creation(self):
        """Test creation and loading of sample Excel file"""
        # Create temporary Excel file with sample data
        sample_data = {
            'Search Table': pd.DataFrame({
                'searchId': [1, 2, 3],
                'search_status': ['P', 'C', 'R'],
                'search_type_code': ['EDU', 'EMP', 'CRI']
            }),
            'Subject Table': pd.DataFrame({
                'subject_id': [101, 102, 103],
                'subject_name': ['John Doe', 'Jane Smith', 'Bob Johnson']
            })
        }

        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
            try:
                with pd.ExcelWriter(temp_file.name, engine='openpyxl') as writer:
                    for sheet_name, df in sample_data.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Test file validation
                self.assertTrue(self.db_connector.validate_excel_file(temp_file.name))

                # Test loading
                success = self.db_connector.load_excel_to_sql(temp_file.name)
                self.assertTrue(success)

                # Verify tables were created
                for table_name in sample_data.keys():
                    self.assertTrue(self.db_connector.check_table_exists(table_name))

            finally:
                # Clean up temporary file
                if os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)


def create_test_suite():
    """Create comprehensive test suite"""
    test_suite = unittest.TestSuite()

    # Add all test classes
    test_classes = [
        TestDatabase,
        TestLLMQueryEngine,
        TestDataVisualizer,
        TestExcelLoading
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    return test_suite


if __name__ == '__main__':
    # Run individual test
    unittest.main(verbosity=2)

    # Or run full test suite
    # runner = unittest.TextTestRunner(verbosity=2)
    # runner.run(create_test_suite())