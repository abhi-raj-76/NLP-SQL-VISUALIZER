from src.database_connection import db_connector
from src.llm_setup import LLMQueryEngine
from src.visualizations import DataVisualizer
from src.utils.chart_utils import create_auto_visualization
import streamlit as st
import pandas as pd
import plotly.express as px  # ADD THIS IMPORT
import plotly.graph_objects as go  # ADD THIS IMPORT

class EnhancedBackgroundCheckChatbot:
    def __init__(self):
        self.query_engine = LLMQueryEngine(db_connector.engine)
        self.visualizer = DataVisualizer(db_connector.engine)

    def process_query(self, user_input):
        """Process user query and return formatted response with visualization"""
        # FIX: Now unpacking 3 values instead of 2
        response, sql_query, df = self.query_engine.query(user_input)

        if "Error" in response:
            return response, None, None

        # Try to get DataFrame for visualization (df is now directly available)
        visualization = self._generate_visualization(user_input, df)

        formatted_response = self._format_response(response, sql_query)  # Pass sql_query instead of steps
        return formatted_response, df, visualization

    def _generate_visualization(self, user_input, df):
        """Generate appropriate visualization based on ANY query results"""
        if df is None or df.empty:
            return None

        # Make sure we have a DataFrame with data
        if not isinstance(df, pd.DataFrame) or len(df) == 0:
            return None

        user_input_lower = user_input.lower()

        # Auto-detect the best visualization based on data columns
        if 'search_status' in df.columns:
            # Status distribution chart
            status_counts = df['search_status'].value_counts()
            fig = px.pie(values=status_counts.values, names=status_counts.index,
                         title=f"Status Distribution for: {user_input[:30]}...")
            return fig

        elif 'comp_name' in df.columns:
            # Company distribution chart
            comp_counts = df['comp_name'].value_counts().head(10)
            fig = px.bar(x=comp_counts.index, y=comp_counts.values,
                         title=f"Company Distribution for: {user_input[:30]}...",
                         labels={'x': 'Company', 'y': 'Count'})
            fig.update_layout(xaxis_tickangle=-45)
            return fig

        elif 'search_type' in df.columns:
            # Search type distribution
            type_counts = df['search_type'].value_counts()
            fig = px.bar(x=type_counts.index, y=type_counts.values,
                         title=f"Search Type Distribution for: {user_input[:30]}...")
            return fig

        elif 'state_code' in df.columns:
            # Geographical map
            state_counts = df['state_code'].value_counts()
            fig = px.choropleth(locations=state_counts.index, locationmode="USA-states",
                                color=state_counts.values, scope="usa",
                                title=f"Geographical Distribution for: {user_input[:30]}...")
            return fig
        elif 'subject_name' in df.columns:
            # Show candidate list as table
            fig = go.Figure(data=[go.Table(
                header=dict(values=list(df.columns),
                            fill_color='paleturquoise',
                            align='left'),
                cells=dict(values=[df[col] for col in df.columns],
                           fill_color='lavender',
                           align='left'))
            ])
            fig.update_layout(title=f"Candidates for: {user_input[:30]}...")
            return fig
        else:
            # Default table view for any other data
            fig = go.Figure(data=[go.Table(
                header=dict(values=list(df.columns),
                            fill_color='paleturquoise',
                            align='left'),
                cells=dict(values=[df[col] for col in df.columns],
                           fill_color='lavender',
                           align='left'))
            ])
            fig.update_layout(title=f"Results for: {user_input[:30]}...")
            return fig

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
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<h1 class="main-header">🔍 Background Verification Analytics Dashboard</h1>',
                unsafe_allow_html=True)

    # Initialize database
    excel_file_path = 'data/DataSet_Hackathon.xlsx'
    if not hasattr(st.session_state, 'db_initialized'):
        try:
            with st.spinner("🔄 Loading database from Excel file..."):
                success = db_connector.load_excel_to_sql(excel_file_path)
                if success:
                    st.session_state.db_initialized = True
                    st.success("✅ Database loaded successfully!")
                    # Refresh to apply changes
                    st.rerun()
                else:
                    st.error("❌ Failed to load database. Please check your Excel file.")
                    return
        except Exception as e:
            st.error(f"❌ Error loading database: {str(e)}")
            return

    # Check if database is ready before creating chatbot
    if not hasattr(st.session_state, 'db_initialized') or not st.session_state.db_initialized:
        st.error("Database not loaded. Please check the Excel file path.")
        return

    # Initialize chatbot
    try:
        chatbot = EnhancedBackgroundCheckChatbot()
    except Exception as e:
        st.error(f"❌ Failed to initialize chatbot: {str(e)}")
        return

    # Sidebar for quick insights
    with st.sidebar:
        st.header("📈 Quick Insights")

        if st.button("Show Status Distribution"):
            fig = chatbot.visualizer.create_status_pie_chart()
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for status distribution.")

        if st.button("Show Company Orders"):
            fig = chatbot.visualizer.create_company_bar_chart()
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for company orders.")

        if st.button("Show Search Types"):
            fig = chatbot.visualizer.create_search_type_treemap()
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for search types.")

        if st.button("Show Geographical Map"):
            fig = chatbot.visualizer.create_geographical_map()
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for geographical map.")

    # Main chat interface
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("💬 Chat with your Data")

        # Initialize session state for user input if not exists
        if 'user_input' not in st.session_state:
            st.session_state.user_input = ""

        user_input = st.text_area(
            "Ask your question:",
            placeholder="e.g., Show me all pending education verifications for Amazon candidates",
            height=100,
            value=st.session_state.user_input,
            key="user_input_text"
        )

        if st.button("🚀 Analyze", key="analyze_btn"):
            if user_input.strip():
                with st.spinner("🔍 Analyzing your query and generating insights..."):
                    try:
                        response, df, visualization = chatbot.process_query(user_input)

                        # Display results
                        st.subheader("📋 Results")
                        st.markdown(response)

                        if df is not None and not df.empty:
                            st.subheader("📊 Data Preview")
                            st.dataframe(df.head(10))

                            if visualization is not None:
                                st.subheader("📈 Visualization")
                                st.plotly_chart(visualization, use_container_width=True)
                            else:
                                st.info("ℹ️ No visualization available for this data.")
                        else:
                            st.warning("⚠️ No data found for your query.")

                    except Exception as e:
                        st.error(f"❌ Error processing query: {str(e)}")
            else:
                st.warning("Please enter a question first!")

    with col2:
        st.subheader("💡 Example Queries")
        examples = [
            "Show pending background checks for Amazon",
            "Count completed education verifications",
            "Show criminal checks by state",
            "List all Nestle company orders",
            "Show status distribution for employment verifications",
            "Display package usage by company"
        ]

        for example in examples:
            if st.button(example, key=f"ex_{example[:10]}"):
                st.session_state.user_input = example
                # Auto-trigger analysis when example is clicked
                st.rerun()

    # Add footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
        🔍 Background Verification Analytics Dashboard | Powered by Streamlit
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()