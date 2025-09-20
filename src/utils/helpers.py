import pandas as pd
import re
import logging
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_column_names(df):
    """Clean DataFrame column names to be database-friendly"""
    try:
        original_columns = df.columns.tolist()
        cleaned_columns = []

        for i, col in enumerate(original_columns):
            if pd.isna(col) or col == '' or col is None:
                cleaned_columns.append(f'unnamed_column_{i}')
            else:
                # Keep original column name but clean whitespace
                cleaned_col = str(col).strip()
                # Remove any problematic characters for SQL
                cleaned_col = re.sub(r'[^\w\s-]', '', cleaned_col)
                cleaned_columns.append(cleaned_col)

        df.columns = cleaned_columns
        logger.info(f"Column names cleaned: {len(cleaned_columns)} columns processed")
        return df
    except Exception as e:
        logger.error(f"Error cleaning column names: {str(e)}")
        return df


def validate_sql_query(query):
    """Enhanced SQL query validation for security"""
    if not query or not isinstance(query, str):
        return False

    # Dangerous patterns to check for
    dangerous_patterns = [
        r';.*--',  # SQL injection attempts
        r'\bDROP\b',  # DROP statements
        r'\bDELETE\b',  # DELETE statements
        r'\bUPDATE\b',  # UPDATE statements
        r'\bINSERT\b',  # INSERT statements
        r'\bEXEC\b',  # EXEC statements
        r'\bXP_\w+',  # Extended procedures
        r'\bSHUTDOWN\b',  # SHUTDOWN
        r'\bALTER\b',  # ALTER statements
        r'\bCREATE\b',  # CREATE statements
        r'\bTRUNCATE\b',  # TRUNCATE statements
        r'\bGRANT\b',  # GRANT statements
        r'\bREVOKE\b',  # REVOKE statements
        r'@@\w+',  # System variables
        r'sp_\w+',  # System stored procedures
    ]

    query_upper = query.upper()
    for pattern in dangerous_patterns:
        if re.search(pattern, query_upper):
            logger.warning(f"Potentially dangerous SQL pattern detected: {pattern}")
            return False

    # Check for excessive complexity
    if len(query) > 5000:
        logger.warning("Query too long")
        return False

    # Check for too many joins (potential performance issue)
    join_count = len(re.findall(r'\bJOIN\b', query_upper))
    if join_count > 5:
        logger.warning(f"Too many joins detected: {join_count}")
        return False

    logger.info("SQL query passed validation")
    return True


def format_query_results(df, max_rows=20):
    """Format query results for display with better formatting"""
    try:
        if df is None or df.empty:
            return "No results found."

        result_summary = f"Found {len(df)} records"

        if len(df) > max_rows:
            result_summary += f" (showing first {max_rows})"
            display_df = df.head(max_rows)
        else:
            display_df = df

        # Create a formatted string representation
        formatted_output = result_summary + "\n\n"

        # Handle wide DataFrames
        max_cols = 8
        if len(display_df.columns) > max_cols:
            cols_to_show = display_df.columns[:max_cols].tolist()
            display_df_subset = display_df[cols_to_show].copy()
            formatted_output += display_df_subset.to_string(index=False)
            formatted_output += f"\n... and {len(display_df.columns) - max_cols} more columns"
        else:
            formatted_output += display_df.to_string(index=False)

        return formatted_output
    except Exception as e:
        logger.error(f"Error formatting query results: {str(e)}")
        return f"Error formatting results: {str(e)}"


def safe_convert_types(df):
    """Safely convert DataFrame column types"""
    try:
        for col in df.columns:
            # Try to convert numeric columns
            if df[col].dtype == 'object':
                # Check if it's numeric
                try:
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    # Only convert if we don't lose too much data
                    non_null_orig = df[col].notna().sum()
                    non_null_numeric = numeric_col.notna().sum()
                    if non_null_numeric >= (non_null_orig * 0.8):  # 80% threshold
                        df[col] = numeric_col
                        logger.debug(f"Converted {col} to numeric")
                except Exception:
                    pass  # Keep as object if conversion fails

        logger.info("Data types converted successfully")
        return df
    except Exception as e:
        logger.error(f"Error converting data types: {str(e)}")
        return df


def normalize_status_codes(status_series):
    """Normalize status codes to standard format"""
    try:
        # Define comprehensive status mappings
        status_map = {
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
            'P5': 'OTHER INFORMATION NEEDED',
            'P6': 'PENDING_6',
            'P7': 'PENDING_7',
            'P8': 'PENDING_8',
            'R2': 'RESOLVED_2',
        }

        # Apply mapping where possible, keep original for unmapped values
        normalized = status_series.map(status_map).fillna(status_series)
        return normalized

    except Exception as e:
        logger.error(f"Error normalizing status codes: {str(e)}")
        return status_series


def clean_text_data(text_series):
    """Clean text data by removing extra whitespace and normalizing"""
    try:
        if text_series.dtype != 'object':
            return text_series

        # Clean text data
        cleaned = text_series.astype(str).str.strip()

        # Replace various null representations
        null_values = ['NULL', 'NONE', 'NA', 'null', 'none', 'na', 'N/A', '']
        for null_val in null_values:
            cleaned = cleaned.replace(null_val, pd.NA)

        # Remove extra whitespace
        cleaned = cleaned.str.replace(r'\s+', ' ', regex=True)

        return cleaned
    except Exception as e:
        logger.error(f"Error cleaning text data: {str(e)}")
        return text_series


def validate_dataframe(df, required_columns=None):
    """Validate DataFrame structure and content"""
    try:
        if df is None:
            logger.error("DataFrame is None")
            return False

        if df.empty:
            logger.warning("DataFrame is empty")
            return False

        # Check for required columns
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                logger.warning(f"Missing required columns: {missing_cols}")
                return False

        # Check for completely empty columns
        empty_cols = df.columns[df.isnull().all()].tolist()
        if empty_cols:
            logger.info(f"Found completely empty columns: {empty_cols}")

        logger.info(f"DataFrame validation passed: {len(df)} rows, {len(df.columns)} columns")
        return True

    except Exception as e:
        logger.error(f"Error validating DataFrame: {str(e)}")
        return False


def standardize_phone_numbers(phone_series):
    """Standardize phone number formats"""
    try:
        def clean_phone(phone):
            if pd.isna(phone):
                return phone
            # Remove all non-digits
            digits_only = re.sub(r'[^\d]', '', str(phone))
            # Format as XXX-XXX-XXXX if 10 digits
            if len(digits_only) == 10:
                return f"{digits_only[:3]}-{digits_only[3:6]}-{digits_only[6:]}"
            elif len(digits_only) == 11 and digits_only[0] == '1':
                return f"{digits_only[1:4]}-{digits_only[4:7]}-{digits_only[7:]}"
            else:
                return phone  # Return original if can't standardize

        cleaned = phone_series.apply(clean_phone)
        logger.info("Phone numbers standardized")
        return cleaned

    except Exception as e:
        logger.error(f"Error standardizing phone numbers: {str(e)}")
        return phone_series


def detect_data_types(df):
    """Detect and suggest appropriate data types for DataFrame columns"""
    try:
        suggestions = {}

        for col in df.columns:
            series = df[col]
            non_null_series = series.dropna()

            if len(non_null_series) == 0:
                suggestions[col] = 'object'
                continue

            # Check for numeric data
            numeric_count = 0
            for val in non_null_series.head(100):  # Sample first 100 values
                try:
                    float(val)
                    numeric_count += 1
                except (ValueError, TypeError):
                    pass

            numeric_ratio = numeric_count / min(len(non_null_series), 100)

            if numeric_ratio > 0.8:
                # Check if integers
                try:
                    int_series = pd.to_numeric(non_null_series, errors='coerce')
                    if int_series.equals(int_series.astype(int, errors='ignore')):
                        suggestions[col] = 'int64'
                    else:
                        suggestions[col] = 'float64'
                except:
                    suggestions[col] = 'object'
            else:
                # Check for dates
                date_count = 0
                for val in non_null_series.head(50):
                    try:
                        pd.to_datetime(val)
                        date_count += 1
                    except:
                        pass

                if date_count / min(len(non_null_series), 50) > 0.5:
                    suggestions[col] = 'datetime64'
                else:
                    suggestions[col] = 'object'

        logger.info(f"Data type detection completed for {len(suggestions)} columns")
        return suggestions

    except Exception as e:
        logger.error(f"Error detecting data types: {str(e)}")
        return {}


def create_data_summary(df):
    """Create comprehensive summary of DataFrame"""
    try:
        summary = {
            'shape': df.shape,
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.to_dict(),
            'null_counts': df.isnull().sum().to_dict(),
            'unique_counts': {},
            'sample_values': {}
        }

        # Get unique counts and sample values for each column
        for col in df.columns:
            try:
                summary['unique_counts'][col] = df[col].nunique()
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    sample_size = min(5, len(non_null_values))
                    summary['sample_values'][col] = non_null_values.head(sample_size).tolist()
                else:
                    summary['sample_values'][col] = []
            except Exception as e:
                logger.warning(f"Error processing column {col}: {str(e)}")
                summary['unique_counts'][col] = 0
                summary['sample_values'][col] = []

        return summary

    except Exception as e:
        logger.error(f"Error creating data summary: {str(e)}")
        return {}


def sanitize_for_sql(text):
    """Sanitize text for safe SQL usage"""
    try:
        if pd.isna(text) or text is None:
            return 'NULL'

        # Convert to string and escape single quotes
        sanitized = str(text).replace("'", "''")

        # Remove or replace potentially dangerous characters
        sanitized = re.sub(r'[;\\]', '', sanitized)

        return f"'{sanitized}'"

    except Exception as e:
        logger.error(f"Error sanitizing text for SQL: {str(e)}")
        return "''"


def batch_process_dataframe(df, batch_size=1000, process_func=None):
    """Process large DataFrames in batches to avoid memory issues"""
    try:
        if process_func is None:
            return df

        processed_batches = []

        for start_idx in range(0, len(df), batch_size):
            end_idx = min(start_idx + batch_size, len(df))
            batch = df.iloc[start_idx:end_idx].copy()

            processed_batch = process_func(batch)
            processed_batches.append(processed_batch)

            logger.debug(f"Processed batch {start_idx // batch_size + 1}/{(len(df) - 1) // batch_size + 1}")

        result_df = pd.concat(processed_batches, ignore_index=True)
        logger.info(f"Batch processing completed: {len(result_df)} rows processed")
        return result_df

    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        return df


def export_to_excel(df, filename, sheet_name='Sheet1'):
    """Export DataFrame to Excel with error handling"""
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        logger.info(f"Data exported to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error exporting to Excel: {str(e)}")
        return False


def get_memory_usage(df):
    """Get detailed memory usage information for DataFrame"""
    try:
        memory_info = {
            'total_memory_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'column_memory': df.memory_usage(deep=True).to_dict(),
            'shape': df.shape,
            'dtypes': df.dtypes.to_dict()
        }
        return memory_info
    except Exception as e:
        logger.error(f"Error getting memory usage: {str(e)}")
        return {}