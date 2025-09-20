from sqlalchemy import text
import pandas as pd
import re
from sqlalchemy import inspect  # ADD THIS IMPORT

class LLMQueryEngine:
    def __init__(self, db_engine):
        self.db_engine = db_engine
        self.table_info = self._get_table_info()

    def _get_table_info(self):
        """Get information about all tables and columns"""
        inspector = inspect(self.db_engine)
        table_info = {}
        for table_name in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            table_info[table_name] = columns
        return table_info

    def _generate_sql_from_query(self, user_query):
        """Convert natural language to SQL using pattern matching"""
        user_query = user_query.lower()

        # Pattern matching for common query types - UPDATED WITH CORRECT TABLE NAMES
        patterns = {
            r'show.*pending.*check': """
                SELECT s.SearchId, s.search_status, sub.subject_name, c.comp_name, s.sub_status
                FROM search_table s
                JOIN subject sub ON s.subject_id = sub.subject_id
                JOIN company c ON sub.comp_id = c.comp_id
                WHERE s.search_status = 'pending'
            """,
            r'count.*pending': """
                SELECT COUNT(*) as pending_count 
                FROM search_table 
                WHERE search_status = 'pending'
            """,
            r'show.*completed.*check': """
                SELECT s.SearchId, s.search_status, sub.subject_name, c.comp_name
                FROM search_table s
                JOIN subject sub ON s.subject_id = sub.subject_id
                JOIN company c ON sub.comp_id = c.comp_id
                WHERE s.search_status = 'completed'
            """,
            r'company.*amazon': """
                SELECT s.SearchId, s.search_status, sub.subject_name, c.comp_name
                FROM search_table s
                JOIN subject sub ON s.subject_id = sub.subject_id
                JOIN company c ON sub.comp_id = c.comp_id
                WHERE c.comp_name LIKE '%Amazon%'
            """,
            r'company.*nestle': """
                SELECT s.SearchId, s.search_status, sub.subject_name, c.comp_name
                FROM search_table s
                JOIN subject sub ON s.subject_id = sub.subject_id
                JOIN company c ON sub.comp_id = c.comp_id
                WHERE c.comp_name LIKE '%Nestle%'
            """,
            r'education.*verification': """
                SELECT s.SearchId, st.search_type, s.search_status, sub.subject_name
                FROM search_table s
                JOIN search_type st ON s.search_type_code = st.search_type_code
                JOIN subject sub ON s.subject_id = sub.subject_id
                WHERE st.search_type LIKE '%education%'
            """,
            r'criminal.*check': """
                SELECT s.SearchId, st.search_type, s.search_status, sub.subject_name
                FROM search_table s
                JOIN search_type st ON s.search_type_code = st.search_type_code
                JOIN subject sub ON s.subject_id = sub.subject_id
                WHERE st.search_type LIKE '%criminal%'
            """
        }

        # Find matching pattern
        for pattern, sql_template in patterns.items():
            if re.search(pattern, user_query):
                return sql_template

        # Default fallback query
        return "SELECT * FROM search_table LIMIT 10"

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
                    if len(df) > 0:
                        if 'search_status' in df.columns:
                            status_counts = df['search_status'].value_counts().to_dict()
                            result_str += f"\nStatus breakdown: {status_counts}"
                        if 'comp_name' in df.columns:
                            top_companies = df['comp_name'].value_counts().head(3).to_dict()
                            result_str += f"\nTop companies: {top_companies}"
                else:
                    df = None
                    result_str = "Query executed successfully"

            return result_str, sql_query, df

        except Exception as e:
            return f"Error processing your query: {str(e)}", None, None