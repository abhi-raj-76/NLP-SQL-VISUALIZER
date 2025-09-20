from src.database_connection import db_connector
from src.llm_setup import LLMQueryEngine
from src.visualizations import DataVisualizer
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import logging
import os
from contextlib import contextmanager
from sqlalchemy import text, inspect

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Config:
    """Configuration management with better defaults"""
    # Update this path to your actual Excel file location
    EXCEL_FILE_PATH = os.getenv('EXCEL_FILE_PATH', 'DataSet_Hackathon.xlsx')

    # Table names mapping (using exact Excel sheet names)
    TABLE_NAMES = {
        'search': 'Search Table',
        'subject': 'Subject Table',
        'company': 'Company Table',
        'search_type': 'Search_Type Table',
        'search_status': 'Search_status',
        'package': 'Package Table',
        'order_request': 'Order_Request Table'
    }

    # Security settings
    DANGEROUS_SQL_KEYWORDS = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'INSERT', 'UPDATE', 'CREATE']
    MAX_RESULTS_DISPLAY = 100


class EnhancedBackgroundCheckChatbot:
    def __init__(self):
        """Initialize the chatbot with error handling"""
        try:
            self.query_engine = LLMQueryEngine(db_connector.engine)
            self.visualizer = DataVisualizer(db_connector.engine)
            logger.info("Chatbot initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize chatbot: {str(e)}")
            raise

    def validate_user_input(self, user_input: str) -> bool:
        """Validate user input for safety and content"""
        if not user_input or not user_input.strip():
            return False

        # Check for dangerous SQL keywords
        user_input_upper = user_input.upper()
        for keyword in Config.DANGEROUS_SQL_KEYWORDS:
            if keyword in user_input_upper:
                logger.warning(f"Potentially dangerous keyword '{keyword}' detected in input: {user_input}")
                return False

        # Check for excessively long input
        if len(user_input) > 1000:
            logger.warning(f"Input too long: {len(user_input)} characters")
            return False

        return True

    def process_query(self, user_input):
        """Process user query with comprehensive error handling"""
        try:
            # Validate input first
            if not self.validate_user_input(user_input):
                error_msg = "Invalid or potentially unsafe query. Please rephrase your question."
                logger.warning(f"Query validation failed for: {user_input}")
                return error_msg, None, None

            # Process the query
            response, sql_query, df = self.query_engine.query(user_input)

            if "Error" in response:
                logger.error(f"Query error: {response}")
                return response, None, None

            # Generate visualization if data is available
            visualization = None
            if df is not None and not df.empty:
                visualization = self._generate_visualization(user_input, df)

            formatted_response = self._format_response(response, sql_query)
            return formatted_response, df, visualization

        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            logger.error(error_msg)
            return error_msg, None, None

    def _generate_visualization(self, user_input, df):
        """Generate appropriate visualization based on query results"""
        try:
            if not isinstance(df, pd.DataFrame) or df is None or df.empty:
                logger.warning("No valid DataFrame for visualization")
                return None

            # Use the auto-visualize function from DataVisualizer
            visualization = self.visualizer.auto_visualize(df, f"Results for: {user_input[:50]}...")

            if visualization is None:
                logger.info("No suitable visualization generated")
            else:
                logger.info("Visualization generated successfully")

            return visualization

        except Exception as e:
            logger.error(f"Error generating visualization: {str(e)}")
            return None

    def _format_response(self, response, sql_query):
        """Format the response with SQL query information"""
        if sql_query:
            return f"""
**Query Results:**
{response}

**SQL Generated:**
```sql
{sql_query}
```
"""
        return response


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = db_connector.engine.connect()
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def safe_database_operation(func):
    """Decorator for safe database operations"""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Database operation failed: {str(e)}")
            st.error(f"Database error: {str(e)}")
            return None

    return wrapper


def initialize_database():
    """Initialize database with comprehensive error handling"""
    if hasattr(st.session_state, 'db_initialized') and st.session_state.db_initialized:
        return True

    excel_file_path = Config.EXCEL_FILE_PATH

    # Check if file exists
    if not os.path.exists(excel_file_path):
        st.error(f"Excel file not found at: {excel_file_path}")
        st.info("Please ensure the Excel file exists in the specified path.")

        # Provide file upload option
        uploaded_file = st.file_uploader(
            "Upload your Excel file",
            type=['xlsx', 'xls'],
            help="Upload your DataSet_Hackathon.xlsx file here"
        )

        if uploaded_file:
            try:
                # Save uploaded file temporarily
                temp_path = "temp_dataset.xlsx"
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                Config.EXCEL_FILE_PATH = temp_path
                st.success("File uploaded successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error uploading file: {str(e)}")

        return False

    try:
        with st.spinner("Loading database from Excel file..."):
            success = db_connector.load_excel_to_sql(excel_file_path)
            if success:
                st.session_state.db_initialized = True
                st.success("Database loaded successfully!")
                return True
            else:
                st.error("Failed to load database. Please check your Excel file format.")
                return False
    except Exception as e:
        error_msg = f"Error loading database: {str(e)}"
        st.error(error_msg)
        logger.error(error_msg)
        return False


@safe_database_operation
def test_database_query(table_name, limit=5):
    """Safely test database queries"""
    try:
        return db_connector.test_query(table_name, limit)
    except Exception as e:
        logger.error(f"Test query failed for table '{table_name}': {str(e)}")
        return None


def display_database_info():
    """Display database information in sidebar"""
    try:
        inspector = inspect(db_connector.engine)
        actual_tables = inspector.get_table_names()

        with st.sidebar:
            st.markdown("### Database Information")
            st.markdown(f"**Tables found:** {len(actual_tables)}")

            with st.expander("View Table Details", expanded=False):
                for table in actual_tables:
                    st.markdown(f"**{table}**")
                    try:
                        columns = inspector.get_columns(table)
                        column_names = [col['name'] for col in columns]
                        st.markdown(f"*Columns:* {', '.join(column_names[:5])}{'...' if len(column_names) > 5 else ''}")

                        # Get row count
                        with get_db_connection() as conn:
                            count_query = text(f'SELECT COUNT(*) as count FROM "{table}"')
                            result = conn.execute(count_query)
                            row_count = result.fetchone()[0]
                            st.markdown(f"*Rows:* {row_count}")
                    except Exception as e:
                        st.markdown(f"*Error:* {str(e)}")
                    st.markdown("---")

    except Exception as e:
        st.sidebar.error(f"Error inspecting database: {e}")
        logger.error(f"Error inspecting database: {str(e)}")


def main():
    """Main Streamlit application"""
    st.set_page_config(
        page_title="Background Verification Analytics Dashboard",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1.5rem;
        padding: 1rem;
        background: linear-gradient(90deg, #f0f2f6, #ffffff);
        border-radius: 10px;
        border: 1px solid #e0e6ed;
    }
    .stButton button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem;
        font-weight: bold;
    }
    .stButton button:hover {
        background-color: #45a049;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .success-message {
        background-color: #d4edda;
        color: #155724;
        padding: 0.75rem;
        border-radius: 0.25rem;
        border: 1px solid #c3e6cb;
    }
    .error-message {
        background-color: #f8d7da;
        color: #721c24;
        padding: 0.75rem;
        border-radius: 0.25rem;
        border: 1px solid #f5c6cb;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<h1 class="main-header">🔍 Background Verification Analytics Dashboard</h1>',
                unsafe_allow_html=True)

    # Initialize session state
    if 'user_input' not in st.session_state:
        st.session_state.user_input = ""
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []

    # Initialize database
    if not initialize_database():
        st.stop()

    # Display database information
    display_database_info()

    # Initialize chatbot
    try:
        if 'chatbot' not in st.session_state:
            st.session_state.chatbot = EnhancedBackgroundCheckChatbot()
        chatbot = st.session_state.chatbot
    except Exception as e:
        st.error(f"Failed to initialize chatbot: {str(e)}")
        logger.error(f"Error initializing chatbot: {str(e)}")
        st.stop()

    # Sidebar for quick actions and tests
    with st.sidebar:
        st.header("🔧 Quick Actions")

        # Database test queries
        st.subheader("Database Tests")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Test Search Table", key="test_search"):
                df = test_database_query('Search Table')
                if df is not None and not df.empty:
                    st.success(f"✅ {len(df)} rows")
                    st.dataframe(df, use_container_width=True)
                else:
                    st.warning("⚠️ No data")

        with col2:
            if st.button("Test Subject Table", key="test_subject"):
                df = test_database_query('Subject Table')
                if df is not None and not df.empty:
                    st.success(f"✅ {len(df)} rows")
                    st.dataframe(df, use_container_width=True)
                else:
                    st.warning("⚠️ No data")

        # Quick visualizations
        st.subheader("Quick Visualizations")
        if st.button("Status Distribution", key="quick_status"):
            try:
                fig = chatbot.visualizer.create_status_pie_chart()
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No data for visualization")
            except Exception as e:
                st.error(f"Error: {str(e)}")

        if st.button("Search Types", key="quick_types"):
            try:
                fig = chatbot.visualizer.create_search_type_bar_chart()
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No data for visualization")
            except Exception as e:
                st.error(f"Error: {str(e)}")

        # System actions
        st.subheader("System Actions")
        if st.button("🔄 Refresh Database", key="refresh_db"):
            for key in ['db_initialized', 'chatbot']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

    # Main interface
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("💬 Ask Your Question")

        # User input area
        user_input = st.text_area(
            "Enter your question about the background check data:",
            placeholder="e.g., 'Show me all pending background checks' or 'Count completed education verifications'",
            height=120,
            value=st.session_state.user_input,
            key="user_input_area"
        )

        # Update session state
        if user_input != st.session_state.user_input:
            st.session_state.user_input = user_input

        # Action buttons
        col_analyze, col_clear = st.columns([3, 1])

        with col_analyze:
            analyze_clicked = st.button("🚀 Analyze Query", key="analyze_btn", use_container_width=True)

        with col_clear:
            if st.button("🗑️ Clear", key="clear_btn", use_container_width=True):
                st.session_state.user_input = ""
                st.rerun()

        # Process query
        if analyze_clicked and user_input.strip():
            with st.spinner("🔍 Analyzing your query..."):
                try:
                    response, df, visualization = chatbot.process_query(user_input)

                    # Add to chat history
                    st.session_state.chat_history.append({
                        'query': user_input,
                        'response': response,
                        'timestamp': pd.Timestamp.now()
                    })

                    # Display results
                    st.subheader("📋 Results")

                    if "Error" in response or "Invalid" in response:
                        st.error(response)
                    else:
                        st.success("Query executed successfully!")
                        st.markdown(response)

                        if df is not None and not df.empty:
                            st.subheader("📊 Data Preview")
                            st.info(f"Found {len(df)} records")

                            # Display limited rows for performance
                            display_rows = min(20, len(df))
                            st.dataframe(df.head(display_rows), use_container_width=True)

                            if len(df) > display_rows:
                                st.info(f"Showing first {display_rows} rows of {len(df)} total")

                            # Show visualization
                            if visualization is not None:
                                st.subheader("📈 Visualization")
                                st.plotly_chart(visualization, use_container_width=True)

                            # Download option
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Results as CSV",
                                data=csv,
                                file_name=f"query_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning("⚠️ No data returned from query")

                except Exception as e:
                    st.error(f"❌ Error processing query: {str(e)}")
                    logger.error(f"Error processing query '{user_input}': {str(e)}")

        elif analyze_clicked:
            st.warning("Please enter a question first!")

    with col2:
        st.subheader("💡 Example Queries")
        examples = [
            "Show all pending background checks",
            "Count completed education verifications",
            "Display status distribution",
            "List all subjects",
            "Show company information",
            "Count records by search type",
            "Show geographical distribution",
            "List recent searches",
            "Display package pricing",
            "Show Amazon related searches"
        ]

        for i, example in enumerate(examples):
            if st.button(example, key=f"example_{i}"):
                st.session_state.user_input = example
                st.rerun()

        # Chat history
        if st.session_state.chat_history:
            st.subheader("📜 Recent Queries")
            with st.expander("View Query History", expanded=False):
                for i, item in enumerate(reversed(st.session_state.chat_history[-5:])):
                    st.markdown(f"**Q{len(st.session_state.chat_history) - i}:** {item['query'][:50]}...")
                    st.markdown(f"*{item['timestamp'].strftime('%H:%M:%S')}*")
                    st.markdown("---")

    # Footer
    st.markdown("---")
    col_footer1, col_footer2, col_footer3 = st.columns(3)

    with col_footer1:
        st.markdown("🔍 **Background Verification Analytics**")
        st.markdown("*Powered by Streamlit & AI*")

    with col_footer2:
        if hasattr(st.session_state, 'db_initialized') and st.session_state.db_initialized:
            st.markdown("✅ **Database Status: Connected**")
        else:
            st.markdown("❌ **Database Status: Not Connected**")

    with col_footer3:
        st.markdown("📈 **Features: Query • Visualize • Download**")


if __name__ == "__main__":
    main()