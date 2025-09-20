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
    """Configuration management"""
    EXCEL_FILE_PATH = os.getenv('EXCEL_FILE_PATH', 'data/DataSet_Hackathon.xlsx')
    TABLE_NAMES = {
        'search': 'Search Table',
        'subject': 'Subject Table'
    }
    DANGEROUS_SQL_KEYWORDS = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'INSERT', 'UPDATE', 'CREATE']


class EnhancedBackgroundCheckChatbot:
    def __init__(self):
        try:
            self.query_engine = LLMQueryEngine(db_connector.engine)
            self.visualizer = DataVisualizer(db_connector.engine)
            logger.info("Chatbot initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize chatbot: {str(e)}")
            raise

    def validate_user_input(self, user_input: str) -> bool:
        """Validate user input for safety"""
        if not user_input or not user_input.strip():
            return False

        # Check for dangerous SQL keywords
        user_input_upper = user_input.upper()
        for keyword in Config.DANGEROUS_SQL_KEYWORDS:
            if keyword in user_input_upper:
                logger.warning(f"Potentially dangerous keyword '{keyword}' detected in input: {user_input}")
                return False

        return True

    def process_query(self, user_input):
        """Process user query and return formatted response with visualization"""
        try:
            # Validate input first
            if not self.validate_user_input(user_input):
                error_msg = "Invalid or potentially unsafe query. Please rephrase your question."
                logger.warning(f"Query validation failed for: {user_input}")
                return error_msg, None, None

            response, sql_query, df = self.query_engine.query(user_input)

            if "Error" in response:
                logger.error(f"Query error: {response}")
                return response, None, None

            # Generate visualization
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
            # Validate input is a DataFrame
            if not isinstance(df, pd.DataFrame) or df is None or df.empty:
                logger.warning(f"No valid DataFrame for visualization: {user_input}")
                return None

            user_input_lower = user_input.lower()

            # Auto-detect the best visualization based on data columns
            if 'search_status' in df.columns:
                # Status distribution chart
                status_counts = df['search_status'].value_counts()
                if not status_counts.empty:
                    fig = px.pie(
                        values=status_counts.values,
                        names=status_counts.index,
                        title=f"Status Distribution for: {user_input[:30]}..."
                    )
                    return fig

            elif 'comp_name' in df.columns:
                # Company distribution chart
                comp_counts = df['comp_name'].value_counts().head(10)
                if not comp_counts.empty:
                    fig = px.bar(
                        x=comp_counts.index,
                        y=comp_counts.values,
                        title=f"Company Distribution for: {user_input[:30]}...",
                        labels={'x': 'Company', 'y': 'Count'}
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    return fig

            elif 'search_type' in df.columns:
                # Search type distribution
                type_counts = df['search_type'].value_counts()
                if not type_counts.empty:
                    fig = px.bar(
                        x=type_counts.index,
                        y=type_counts.values,
                        title=f"Search Type Distribution for: {user_input[:30]}..."
                    )
                    return fig

            elif 'state_code' in df.columns:
                # Geographical map
                state_counts = df['state_code'].value_counts()
                if not state_counts.empty:
                    fig = px.choropleth(
                        locations=state_counts.index,
                        locationmode="USA-states",
                        color=state_counts.values,
                        scope="usa",
                        title=f"Geographical Distribution for: {user_input[:30]}..."
                    )
                    return fig

            elif 'subject_name' in df.columns:
                # Show candidate list as table
                if len(df) > 0:
                    fig = go.Figure(data=[go.Table(
                        header=dict(
                            values=list(df.columns),
                            fill_color='paleturquoise',
                            align='left'
                        ),
                        cells=dict(
                            values=[df[col] for col in df.columns],
                            fill_color='lavender',
                            align='left'
                        )
                    )])
                    fig.update_layout(title=f"Candidates for: {user_input[:30]}...")
                    return fig

            else:
                # Default table view for any other data
                if len(df) > 0:
                    # Limit to first 100 rows for performance
                    display_df = df.head(100)
                    fig = go.Figure(data=[go.Table(
                        header=dict(
                            values=list(display_df.columns),
                            fill_color='paleturquoise',
                            align='left'
                        ),
                        cells=dict(
                            values=[display_df[col] for col in display_df.columns],
                            fill_color='lavender',
                            align='left'
                        )
                    )])
                    fig.update_layout(title=f"Results for: {user_input[:30]}...")
                    return fig

            return None

        except Exception as e:
            logger.error(f"Error generating visualization for query '{user_input}': {str(e)}")
            return None

    def _format_response(self, response, sql_query):
        """Format the LLM response"""
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
    """Initialize database with proper error handling"""
    if hasattr(st.session_state, 'db_initialized') and st.session_state.db_initialized:
        return True

    excel_file_path = Config.EXCEL_FILE_PATH

    # Check if file exists
    if not os.path.exists(excel_file_path):
        st.error(f"❌ Excel file not found at: {excel_file_path}")
        st.info("Please ensure the Excel file exists in the specified path.")
        return False

    try:
        with st.spinner("🔄 Loading database from Excel file..."):
            success = db_connector.load_excel_to_sql(excel_file_path)
            if success:
                st.session_state.db_initialized = True
                st.success("✅ Database loaded successfully!")
                return True
            else:
                st.error("❌ Failed to load database. Please check your Excel file format.")
                return False
    except Exception as e:
        error_msg = f"❌ Error loading database: {str(e)}"
        st.error(error_msg)
        logger.error(f"Error loading database: {str(e)}")
        return False


@safe_database_operation
def test_database_query(table_name, limit=5):
    """Safely test database queries"""
    try:
        with get_db_connection() as conn:
            query = text(f'SELECT * FROM "{table_name}" LIMIT :limit')
            result = conn.execute(query, {"limit": limit})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except Exception as e:
        logger.error(f"Test query failed for table '{table_name}': {str(e)}")
        return None


def main():
    st.set_page_config(
        page_title="🔍 Background Verification Analytics Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    .stButton button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<h1 class="main-header">🔍 Background Verification Analytics Dashboard</h1>',
                unsafe_allow_html=True)

    # Initialize session state
    if 'user_input' not in st.session_state:
        st.session_state.user_input = ""

    # Initialize database
    if not initialize_database():
        st.stop()

    # Debug: Check what tables actually exist
    try:
        inspector = inspect(db_connector.engine)
        actual_tables = inspector.get_table_names()

        with st.sidebar:
            st.markdown("📋 **Database Info:**")
            st.markdown(f"**Tables found:** {len(actual_tables)}")

            with st.expander("View Table Details", expanded=False):
                for table in actual_tables:
                    st.markdown(f"**📊 {table}**")
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
                        st.markdown(f"*Error getting details:* {str(e)}")
                    st.markdown("---")

    except Exception as e:
        st.sidebar.error(f"Error inspecting database: {e}")
        logger.error(f"Error inspecting database: {str(e)}")

    # Initialize chatbot
    try:
        if 'chatbot' not in st.session_state:
            st.session_state.chatbot = EnhancedBackgroundCheckChatbot()
        chatbot = st.session_state.chatbot
    except Exception as e:
        st.error(f"❌ Failed to initialize chatbot: {str(e)}")
        logger.error(f"Error initializing chatbot: {str(e)}")
        st.stop()

    # Sidebar for quick insights
    with st.sidebar:
        st.header("📈 Quick Insights")

        # Database test queries
        st.subheader("🧪 Database Test Queries")

        if st.button("Test: Search Table Sample", key="test_search"):
            df = test_database_query(Config.TABLE_NAMES['search'])
            if df is not None and not df.empty:
                st.success(f"✅ Found {len(df)} rows")
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("No data found or error occurred")

        if st.button("Test: Subject Table Sample", key="test_subject"):
            df = test_database_query(Config.TABLE_NAMES['subject'])
            if df is not None and not df.empty:
                st.success(f"✅ Found {len(df)} rows")
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("No data found or error occurred")

        st.header("📊 Quick Visualizations")
        if st.button("Show Status Distribution", key="quick_viz"):
            try:
                fig = chatbot.visualizer.create_status_pie_chart()
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No data available for status distribution.")
            except Exception as e:
                st.error(f"Error creating visualization: {str(e)}")

    # Main interface
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("💬 Chat with your Data")

        # User input area
        user_input = st.text_area(
            "Ask your question:",
            placeholder="e.g., Show me all pending background checks\n(Note: Avoid using dangerous SQL keywords)",
            height=100,
            value=st.session_state.user_input,
            key="user_input_area"
        )

        # Update session state
        if user_input != st.session_state.user_input:
            st.session_state.user_input = user_input

        col_analyze, col_clear = st.columns([3, 1])

        with col_analyze:
            analyze_clicked = st.button("🚀 Analyze", key="analyze_btn", use_container_width=True)

        with col_clear:
            if st.button("🗑️ Clear", key="clear_btn", use_container_width=True):
                st.session_state.user_input = ""
                st.rerun()

        if analyze_clicked:
            if user_input.strip():
                with st.spinner("🔍 Analyzing your query and generating insights..."):
                    try:
                        response, df, visualization = chatbot.process_query(user_input)

                        # Display results
                        st.subheader("📋 Results")

                        # Check if it's an error response
                        if "Error" in response or "Invalid" in response:
                            st.error(response)
                        else:
                            st.markdown(response)

                        if df is not None and not df.empty:
                            st.subheader("📊 Data Preview")
                            st.info(f"Showing {len(df)} rows of data")
                            st.dataframe(df.head(20), use_container_width=True)

                            if visualization is not None:
                                st.subheader("📈 Visualization")
                                st.plotly_chart(visualization, use_container_width=True)
                            else:
                                st.info("ℹ️ No visualization available for this data type.")

                            # Download option
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Results as CSV",
                                data=csv,
                                file_name=f"query_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning("⚠️ No data found for your query.")

                    except Exception as e:
                        st.error(f"❌ Error processing query: {str(e)}")
                        logger.error(f"Error processing query '{user_input}': {str(e)}")
            else:
                st.warning("Please enter a question first!")

    with col2:
        st.subheader("💡 Example Queries")
        examples = [
            "Show pending background checks",
            "Count completed education verifications",
            "Show search status distribution",
            "List all background checks",
            "Count number of subjects",
            "Show recent searches",
            "Display verification types"
        ]

        for i, example in enumerate(examples):
            if st.button(example, key=f"example_{i}"):
                st.session_state.user_input = example
                st.rerun()

        st.subheader("🎯 Quick Actions")

        if st.button("🔄 Refresh Database", key="refresh_db"):
            # Clear database initialization flag
            if hasattr(st.session_state, 'db_initialized'):
                del st.session_state.db_initialized
            if hasattr(st.session_state, 'chatbot'):
                del st.session_state.chatbot
            st.rerun()

        if st.button("📊 View All Tables", key="view_tables"):
            try:
                inspector = inspect(db_connector.engine)
                tables = inspector.get_table_names()
                st.write(f"**Available Tables:** {', '.join(tables)}")
            except Exception as e:
                st.error(f"Error: {str(e)}")

    # Add footer with additional info
    st.markdown("---")
    col_footer1, col_footer2, col_footer3 = st.columns(3)

    with col_footer1:
        st.markdown(
            """
            <div style='text-align: center; color: gray;'>
            🔍 <strong>Background Verification Analytics</strong><br>
            Powered by Streamlit & AI
            </div>
            """,
            unsafe_allow_html=True
        )

    with col_footer2:
        if hasattr(st.session_state, 'db_initialized') and st.session_state.db_initialized:
            st.markdown(
                """
                <div style='text-align: center; color: green;'>
                ✅ <strong>Database Status</strong><br>
                Connected & Ready
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                """
                <div style='text-align: center; color: red;'>
                ❌ <strong>Database Status</strong><br>
                Not Connected
                </div>
                """,
                unsafe_allow_html=True
            )

    with col_footer3:
        st.markdown(
            """
            <div style='text-align: center; color: gray;'>
            📈 <strong>Analytics Ready</strong><br>
            Query • Visualize • Download
            </div>
            """,
            unsafe_allow_html=True
        )


if __name__ == "__main__":
    main()