from sqlalchemy import text, inspect
import pandas as pd
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LLMQueryEngine:
    def __init__(self, db_engine):
        """Initialize LLM Query Engine with database engine"""
        self.db_engine = db_engine
        self.table_info = self._get_table_info()
        logger.info(f"LLMQueryEngine initialized with {len(self.table_info)} tables")

    def _get_table_info(self):
        """Get information about all tables and columns"""
        try:
            inspector = inspect(self.db_engine)
            table_info = {}
            for table_name in inspector.get_table_names():
                columns = [col['name'] for col in inspector.get_columns(table_name)]
                table_info[table_name] = columns
            logger.info(f"Loaded table info: {list(table_info.keys())}")
            return table_info
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return {}

    def _get_status_mapping(self):
        """Get status code mapping from database"""
        status_mapping = {
            'P': 'PENDING',
            'C': 'COMPLETED',
            'R': 'RESOLVED',
            'D': 'DRAFT',
            'F': 'RECORD FOUND',
            'N': 'NO RECORD FOUND',
            'P11': 'Awaiting County Search',
            'P13': 'QUALITY',
            'P14': 'AWAITING ACTION',
            'P4': 'RELEASE NEEDED',
            'P5': 'OTHER INFORMATION NEEDED'
        }

        try:
            # Try to load status mapping from database if Search_status table exists
            if 'Search_status' in self.table_info:
                with self.db_engine.connect() as conn:
                    status_df = pd.read_sql('SELECT * FROM "Search_status"', conn)
                    db_mapping = dict(zip(status_df['Status_code'], status_df['Status']))
                    status_mapping.update(db_mapping)
                    logger.info(f"Loaded {len(db_mapping)} status codes from database")
        except Exception as e:
            logger.warning(f"Could not load status mapping from database: {str(e)}")

        return status_mapping

    def _generate_sql_from_query(self, user_query):
        """Convert natural language to SQL using simplified patterns"""
        user_query_lower = user_query.lower()

        # Table names (using exact Excel sheet names)
        search_table = "Search Table"
        subject_table = "Subject Table"
        company_table = "Company Table"
        search_type_table = "Search_Type Table"
        search_status_table = "Search_status"
        order_request_table = "Order_Request Table"
        package_table = "Package Table"

        # Enhanced SQL patterns with proper error handling
        patterns = [
            # Status-related queries
            {
                'pattern': r'(show|list|get).*pending.*check',
                'sql': f'''
                SELECT s.searchId, s.search_status, s.search_type_code, s.county_name, s.state_code, s.sub_status
                FROM "{search_table}" s
                WHERE s.search_status IN ('P', 'P8', 'P6', 'P7', 'P11', 'P13', 'P14', 'P4', 'P5')
                LIMIT 20
                ''',
                'description': 'Shows pending background checks'
            },
            {
                'pattern': r'count.*pending',
                'sql': f'''
                SELECT COUNT(*) as pending_count 
                FROM "{search_table}"
                WHERE search_status IN ('P', 'P8', 'P6', 'P7', 'P11', 'P13', 'P14', 'P4', 'P5')
                ''',
                'description': 'Counts pending records'
            },
            {
                'pattern': r'(show|display).*status.*distribution',
                'sql': f'''
                SELECT search_status, COUNT(*) as count
                FROM "{search_table}"
                WHERE search_status IS NOT NULL
                GROUP BY search_status
                ORDER BY count DESC
                ''',
                'description': 'Shows status distribution'
            },
            {
                'pattern': r'count.*completed.*education',
                'sql': f'''
                SELECT COUNT(*) as completed_education_count 
                FROM "{search_table}"
                WHERE search_status = 'C' AND search_type_code = 'EDU'
                ''',
                'description': 'Counts completed education verifications'
            },

            # Subject-related queries
            {
                'pattern': r'(count|number).*subject',
                'sql': f'''
                SELECT COUNT(DISTINCT subject_id) as subject_count
                FROM "{subject_table}"
                ''',
                'description': 'Counts unique subjects'
            },
            {
                'pattern': r'(show|list).*subject',
                'sql': f'''
                SELECT subject_id, subject_name, subject_contact, sbj_city
                FROM "{subject_table}"
                WHERE subject_name IS NOT NULL
                LIMIT 20
                ''',
                'description': 'Shows subject information'
            },

            # Company-related queries
            {
                'pattern': r'(show|list).*compan',
                'sql': f'''
                SELECT comp_id, comp_name, comp_code
                FROM "{company_table}"
                ORDER BY comp_name
                LIMIT 20
                ''',
                'description': 'Shows company information'
            },
            {
                'pattern': r'amazon.*search',
                'sql': f'''
                SELECT s.searchId, s.search_status, s.search_type_code
                FROM "{search_table}" s
                JOIN "{order_request_table}" o ON s.subject_id = o.order_subjectId
                JOIN "{company_table}" c ON o.order_CompanyCode = c.comp_code
                WHERE c.comp_name LIKE '%Amazon%' OR c.comp_code LIKE '%AMZ%'
                LIMIT 20
                ''',
                'description': 'Shows Amazon-related searches'
            },

            # General queries
            {
                'pattern': r'(show|list|get).*(all|background|check)',
                'sql': f'''
                SELECT s.searchId, s.search_status, s.search_type_code, s.county_name, s.state_code
                FROM "{search_table}" s
                WHERE s.searchId IS NOT NULL
                ORDER BY s.searchId DESC
                LIMIT 20
                ''',
                'description': 'Shows all background checks'
            },
            {
                'pattern': r'recent.*search',
                'sql': f'''
                SELECT s.searchId, s.search_status, s.search_type_code, s.county_name, s.state_code
                FROM "{search_table}" s
                ORDER BY s.searchId DESC
                LIMIT 10
                ''',
                'description': 'Shows recent searches'
            },
            {
                'pattern': r'verification.*type',
                'sql': f'''
                SELECT search_type_code, search_type, search_type_category
                FROM "{search_type_table}"
                ORDER BY search_type_category, search_type
                ''',
                'description': 'Shows verification types'
            },

            # Package-related queries
            {
                'pattern': r'package.*price',
                'sql': f'''
                SELECT package_code, package_name, package_price, comp_code
                FROM "{package_table}"
                WHERE package_price IS NOT NULL
                ORDER BY package_price DESC
                LIMIT 20
                ''',
                'description': 'Shows package pricing'
            }
        ]

        # Find matching pattern
        for pattern_info in patterns:
            if re.search(pattern_info['pattern'], user_query_lower):
                logger.info(f"Matched pattern: {pattern_info['description']}")
                return pattern_info['sql'].strip(), pattern_info['description']

        # Default fallback query
        default_sql = f'''
        SELECT s.searchId, s.search_status, s.search_type_code, s.county_name, s.state_code
        FROM "{search_table}" s
        LIMIT 10
        '''
        logger.info("No specific pattern matched, using default query")
        return default_sql.strip(), "Default search results"

    def _validate_sql_query(self, sql_query):
        """Basic SQL validation to prevent dangerous operations"""
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE', 'CREATE']
        sql_upper = sql_query.upper()

        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                logger.warning(f"Dangerous SQL keyword '{keyword}' detected")
                return False
        return True

    def query(self, natural_language_query):
        """Process natural language query and return results"""
        try:
            # Input validation
            if not natural_language_query or not natural_language_query.strip():
                return "Please provide a valid query.", None, None

            # Generate SQL query
            sql_query, description = self._generate_sql_from_query(natural_language_query)

            # Validate SQL for safety
            if not self._validate_sql_query(sql_query):
                return "Invalid or potentially dangerous query detected.", None, None

            # Execute the query
            with self.db_engine.connect() as conn:
                result = conn.execute(text(sql_query))

                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())

                    if len(df) == 0:
                        result_str = f"No results found for: {description}"
                    else:
                        result_str = f"Found {len(df)} records - {description}"

                        # Add helpful summary information
                        if 'search_status' in df.columns:
                            status_counts = df['search_status'].value_counts()
                            if len(status_counts) > 0:
                                top_status = status_counts.head(3)
                                result_str += f"\nTop statuses: {dict(top_status)}"

                        if 'search_type_code' in df.columns:
                            type_counts = df['search_type_code'].value_counts()
                            if len(type_counts) > 0:
                                top_types = type_counts.head(3)
                                result_str += f"\nTop types: {dict(top_types)}"
                else:
                    df = pd.DataFrame()
                    result_str = f"Query executed successfully - {description}"

                return result_str, sql_query, df

        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            logger.error(f"Query error for '{natural_language_query}': {str(e)}")
            return error_msg, None, None

    def get_available_tables(self):
        """Get list of available tables"""
        return list(self.table_info.keys())

    def get_table_columns(self, table_name):
        """Get columns for a specific table"""
        return self.table_info.get(table_name, [])

    def get_sample_data(self, table_name, limit=5):
        """Get sample data from a table"""
        try:
            if table_name not in self.table_info:
                return None

            with self.db_engine.connect() as conn:
                query = text(f'SELECT * FROM "{table_name}" LIMIT :limit')
                result = conn.execute(query, {"limit": limit})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            logger.error(f"Error getting sample data from {table_name}: {str(e)}")
            return None