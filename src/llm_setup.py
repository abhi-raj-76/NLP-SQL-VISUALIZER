from sqlalchemy import text, inspect
import pandas as pd
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LLMQueryEngine:
    def __init__(self, db_engine):
        self.db_engine = db_engine
        self.table_info = self._get_table_info()

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

    def _generate_sql_from_query(self, user_query):
        """Convert natural language to SQL using exact table names"""
        user_query = user_query.lower()

        # Use exact table names from Excel sheets
        search_table = "Search Table"
        subject_table = "Subject Table"
        company_table = "Company Table"
        search_type_table = "Search_Type Table"
        search_status_table = "Search_status"
        order_request_table = "Order_Request Table"

        # Handle status codes by mapping known and unknown codes
        status_mapping = {'P': 'PENDING', 'C': 'COMPLETED'}
        try:
            with self.db_engine.connect() as conn:
                status_df = pd.read_sql(f'SELECT status_code, status FROM "{search_status_table}"', conn)
                status_mapping.update(dict(zip(status_df['status_code'], status_df['status'])))
                # Map invalid status codes to consistent values
                invalid_codes = ['R', 'P8', 'R2', 'P6', 'P7']
                for code in invalid_codes:
                    if code not in status_mapping:
                        if code == 'R':
                            status_mapping[code] = 'RESOLVED'
                        elif code.startswith('P'):
                            status_mapping[code] = f'PENDING_{code[1:]}'
                        else:
                            status_mapping[code] = f'RESOLVED_{code[1:]}'
                        logger.warning(f"Status code '{code}' not found in Search_status, defaulting to '{status_mapping[code]}'")
        except Exception as e:
            logger.error(f"Error loading status codes: {str(e)}")
            status_mapping.update({
                'R': 'RESOLVED',
                'P8': 'PENDING_8',
                'R2': 'RESOLVED_2',
                'P6': 'PENDING_6',
                'P7': 'PENDING_7'
            })

        # SQL patterns for user queries
        patterns = {
            r'count.*completed.*education': f"""
                SELECT COUNT(*) as completed_education_count 
                FROM "{search_table}" s
                JOIN "{search_status_table}" ss ON s.search_status = ss.status_code
                WHERE ss.status = 'COMPLETED' 
                AND s.search_type_code = 'EDU'
            """,
            r'show.*pending.*check': f"""
                SELECT s.searchid, ss.status as search_status, sub.subject_name, c.comp_name, s.sub_status
                FROM "{search_table}" s
                JOIN "{subject_table}" sub ON s.subject_id = sub.subject_id
                JOIN "{order_request_table}" o ON s.subject_id = o.order_subjectid
                JOIN "{company_table}" c ON o.order_companycode = c.comp_code
                JOIN "{search_status_table}" ss ON s.search_status = ss.status_code
                WHERE ss.status IN ('PENDING', 'PENDING_8', 'PENDING_6', 'PENDING_7')
            """,
            r'count.*pending': f"""
                SELECT COUNT(*) as pending_count 
                FROM "{search_table}" s
                JOIN "{search_status_table}" ss ON s.search_status = ss.status_code
                WHERE ss.status IN ('PENDING', 'PENDING_8', 'PENDING_6', 'PENDING_7')
            """,
            r'company.*amazon': f"""
                SELECT s.searchid, ss.status as search_status, sub.subject_name, c.comp_name
                FROM "{search_table}" s
                JOIN "{subject_table}" sub ON s.subject_id = sub.subject_id
                JOIN "{order_request_table}" o ON s.subject_id = o.order_subjectid
                JOIN "{company_table}" c ON o.order_companycode = c.comp_code
                JOIN "{search_status_table}" ss ON s.search_status = ss.status_code
                WHERE c.comp_name LIKE '%Amazon%' OR c.comp_code LIKE '%AMZ%'
            """,
            r'list.*background.*check': f"""
                SELECT s.searchid, ss.status as search_status, sub.subject_name, c.comp_name, s.sub_status
                FROM "{search_table}" s
                JOIN "{subject_table}" sub ON s.subject_id = sub.subject_id
                JOIN "{order_request_table}" o ON s.subject_id = o.order_subjectid
                JOIN "{company_table}" c ON o.order_companycode = c.comp_code
                JOIN "{search_status_table}" ss ON s.search_status = ss.status_code
                LIMIT 10
            """,
            r'count.*subject': f"""
                SELECT COUNT(DISTINCT s.subject_id) as subject_count
                FROM "{search_table}" s
                JOIN "{subject_table}" sub ON s.subject_id = sub.subject_id
            """
        }

        # Find matching pattern
        for pattern, sql_template in patterns.items():
            if re.search(pattern, user_query):
                logger.info(f"Generated SQL for query '{user_query}': {sql_template}")
                return sql_template

        # Default fallback query
        default_query = f'SELECT * FROM "{search_table}" LIMIT 10'
        logger.info(f"No pattern matched for query '{user_query}', using default: {default_query}")
        return default_query

    def query(self, natural_language_query):
        """Handle any user query and return appropriate results"""
        try:
            # Generate SQL based on user query
            sql_query = self._generate_sql_from_query(natural_language_query)

            # Execute the query
            with self.db_engine.connect() as conn:
                result = conn.execute(text(sql_query))

                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    result_str = f"Found {len(df)} records for your query"

                    # Add summary for better user experience
                    if len(df) > 0 and 'search_status' in df.columns:
                        status_counts = df['search_status'].value_counts().to_dict()
                        result_str += f"\nStatus breakdown: {status_counts}"
                else:
                    df = None
                    result_str = "Query executed successfully"

            return result_str, sql_query, df

        except Exception as e:
            logger.error(f"Error processing query '{natural_language_query}': {str(e)}")
            return f"Error processing your query: {str(e)}", None, None