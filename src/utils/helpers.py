import pandas as pd
import re


def clean_column_names(df):
    """Clean DataFrame column names"""
    df.columns = [col.strip().lower().replace(' ', '_').replace('(', '').replace(')', '')
                  for col in df.columns if col]
    return df


def validate_sql_query(query):
    """Basic SQL query validation"""
    # Simple validation - prevent obvious SQL injection patterns
    dangerous_patterns = [
        r';.*--', r'DROP', r'DELETE', r'UPDATE', r'INSERT',
        r'EXEC', r'XP_', r'SHUTDOWN'
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, query, re.IGNORECASE):
            return False
    return True


def format_query_results(df, max_rows=10):
    """Format query results for display"""
    if df is None or df.empty:
        return "No results found."

    if len(df) > max_rows:
        return f"Showing first {max_rows} of {len(df)} results:\n" + df.head(max_rows).to_string()
    else:
        return df.to_string()