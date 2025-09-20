import pandas as pd
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_column_names(df):
    """Clean DataFrame column names"""
    try:
        df.columns = [col.strip().lower().replace(' ', '_').replace('(', '').replace(')', '')
                      for col in df.columns if col]
        logger.info("Column names cleaned successfully")
        return df
    except Exception as e:
        logger.error(f"Error cleaning column names: {str(e)}")
        return df

def validate_sql_query(query):
    """Basic SQL query validation"""
    dangerous_patterns = [
        r';.*--', r'DROP', r'DELETE', r'UPDATE', r'INSERT',
        r'EXEC', r'XP_', r'SHUTDOWN'
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, query, re.IGNORECASE):
            logger.warning(f"Potentially dangerous SQL pattern detected: {pattern}")
            return False
    logger.info("SQL query passed basic validation")
    return True

def format_query_results(df, max_rows=10):
    """Format query results for display"""
    if df is None or df.empty:
        return "No results found."

    if len(df) > max_rows:
        return f"Showing first {max_rows} of {len(df)} results:\n" + df.head(max_rows).to_string()
    else:
        return df.to_string()