import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text, inspect
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataVisualizer:
    def __init__(self, engine):
        """Initialize Data Visualizer with database engine"""
        self.engine = engine
        logger.info("DataVisualizer initialized")

    def check_table_exists(self, table_name):
        """Check if a table exists in the database"""
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            return False

    def execute_query(self, query):
        """Execute SQL query and return DataFrame with error handling"""
        try:
            with self.engine.connect() as conn:
                if isinstance(query, str):
                    query = text(query)
                result = conn.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return pd.DataFrame()

    def create_status_pie_chart(self):
        """Create pie chart of search status distribution"""
        try:
            if not self.check_table_exists('Search Table'):
                logger.warning("Search Table not found")
                return None

            query = '''
            SELECT search_status, COUNT(*) as count 
            FROM "Search Table"
            WHERE search_status IS NOT NULL
            GROUP BY search_status
            ORDER BY count DESC
            '''

            df = self.execute_query(query)

            if df.empty:
                logger.warning("No data found for status chart")
                return None

            # Create pie chart
            fig = px.pie(df, values='count', names='search_status',
                         title='Background Check Status Distribution',
                         labels={'search_status': 'Status', 'count': 'Count'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(showlegend=True, font=dict(size=12))

            return fig

        except Exception as e:
            logger.error(f"Error creating status pie chart: {str(e)}")
            return None

    def create_search_type_bar_chart(self):
        """Create bar chart of search type distribution"""
        try:
            if not self.check_table_exists('Search Table'):
                logger.warning("Search Table not found")
                return None

            query = '''
            SELECT search_type_code, COUNT(*) as count
            FROM "Search Table"
            WHERE search_type_code IS NOT NULL
            GROUP BY search_type_code
            ORDER BY count DESC
            LIMIT 15
            '''

            df = self.execute_query(query)

            if df.empty:
                logger.warning("No data found for search type chart")
                return None

            fig = px.bar(df, x='search_type_code', y='count',
                         title='Background Check Types Distribution (Top 15)',
                         labels={'search_type_code': 'Search Type', 'count': 'Count'})

            fig.update_layout(xaxis_tickangle=-45, showlegend=False)
            return fig

        except Exception as e:
            logger.error(f"Error creating search type bar chart: {str(e)}")
            return None

    def create_company_bar_chart(self):
        """Create bar chart of orders by company"""
        try:
            required_tables = ['Order_Request Table', 'Company Table']
            for table in required_tables:
                if not self.check_table_exists(table):
                    logger.warning(f"Required table '{table}' not found")
                    return None

            query = '''
            SELECT c.comp_name, COUNT(*) as order_count
            FROM "Order_Request Table" o
            JOIN "Company Table" c ON o.order_CompanyCode = c.comp_code
            GROUP BY c.comp_name, c.comp_code
            ORDER BY order_count DESC
            LIMIT 10
            '''

            df = self.execute_query(query)

            if df.empty:
                logger.warning("No data found for company chart")
                return None

            fig = px.bar(df, x='comp_name', y='order_count',
                         title='Top 10 Companies by Number of Orders',
                         labels={'comp_name': 'Company', 'order_count': 'Number of Orders'})

            fig.update_layout(xaxis_tickangle=-45)
            return fig

        except Exception as e:
            logger.error(f"Error creating company bar chart: {str(e)}")
            return None

    def create_geographical_map(self):
        """Create geographical distribution map"""
        try:
            if not self.check_table_exists('Search Table'):
                logger.warning("Search Table not found")
                return None

            query = '''
            SELECT state_code, COUNT(*) as count
            FROM "Search Table"
            WHERE state_code IS NOT NULL 
            AND state_code NOT IN ('NA', 'NONE', '', 'NULL')
            AND LENGTH(state_code) = 2
            GROUP BY state_code
            ORDER BY count DESC
            '''

            df = self.execute_query(query)

            if df.empty:
                logger.warning("No valid state data found for geographical map")
                return None

            fig = px.choropleth(df,
                                locations='state_code',
                                locationmode="USA-states",
                                color='count',
                                scope="usa",
                                title='Background Checks Distribution by State',
                                labels={'count': 'Number of Checks', 'state_code': 'State'},
                                color_continuous_scale='Blues')

            fig.update_layout(geo=dict(showlakes=True, lakecolor='rgb(255, 255, 255)'))
            return fig

        except Exception as e:
            logger.error(f"Error creating geographical map: {str(e)}")
            return None

    def create_package_price_analysis(self):
        """Analyze package prices and usage"""
        try:
            required_tables = ['Package Table', 'Company Table', 'Order_Request Table']
            for table in required_tables:
                if not self.check_table_exists(table):
                    logger.warning(f"Required table '{table}' not found")
                    return None

            query = '''
            SELECT p.package_name, p.package_price, c.comp_name, 
                   COUNT(o.order_id) as usage_count
            FROM "Package Table" p
            JOIN "Company Table" c ON p.comp_code = c.comp_code
            LEFT JOIN "Order_Request Table" o ON p.package_code = o.Order_packageCode
            WHERE p.package_price IS NOT NULL AND p.package_price > 0
            GROUP BY p.package_name, p.package_price, c.comp_name
            HAVING usage_count > 0
            ORDER BY usage_count DESC
            LIMIT 20
            '''

            df = self.execute_query(query)

            if df.empty:
                logger.warning("No package data found for analysis")
                return None

            fig = px.scatter(df, x='package_price', y='usage_count',
                             size='usage_count', color='comp_name',
                             hover_name='package_name',
                             title='Package Price vs Usage Analysis',
                             labels={'package_price': 'Package Price ($)',
                                     'usage_count': 'Number of Orders',
                                     'comp_name': 'Company'})

            fig.update_layout(showlegend=True)
            return fig

        except Exception as e:
            logger.error(f"Error creating package price analysis: {str(e)}")
            return None

    def create_search_type_treemap(self):
        """Create treemap of search types by category"""
        try:
            required_tables = ['Search Table', 'Search_Type Table']
            for table in required_tables:
                if not self.check_table_exists(table):
                    logger.warning(f"Required table '{table}' not found")
                    return None

            query = '''
            SELECT st.search_type_category, st.search_type, COUNT(*) as count
            FROM "Search Table" s
            JOIN "Search_Type Table" st ON s.search_type_code = st.search_type_code
            WHERE st.search_type_category IS NOT NULL 
            AND st.search_type IS NOT NULL
            GROUP BY st.search_type_category, st.search_type
            ORDER BY count DESC
            '''

            df = self.execute_query(query)

            if df.empty:
                logger.warning("No search type data found for treemap")
                return None

            fig = px.treemap(df, path=['search_type_category', 'search_type'], values='count',
                             title='Search Types Distribution by Category',
                             labels={'count': 'Number of Searches'})

            fig.update_layout(font=dict(size=12))
            return fig

        except Exception as e:
            logger.error(f"Error creating search type treemap: {str(e)}")
            return None

    def create_status_timeline(self):
        """Create timeline of status changes if date information is available"""
        try:
            if not self.check_table_exists('Search Table'):
                logger.warning("Search Table not found")
                return None

            # This is a placeholder - actual implementation would need date columns
            query = '''
            SELECT search_status, COUNT(*) as count
            FROM "Search Table"
            WHERE search_status IS NOT NULL
            GROUP BY search_status
            ORDER BY search_status
            '''

            df = self.execute_query(query)

            if df.empty:
                return None

            fig = px.line(df, x='search_status', y='count',
                          title='Status Distribution Overview',
                          labels={'search_status': 'Status', 'count': 'Count'})

            return fig

        except Exception as e:
            logger.error(f"Error creating status timeline: {str(e)}")
            return None

    def create_data_summary_table(self, df, title="Data Summary"):
        """Create a formatted table visualization for any DataFrame"""
        try:
            if df is None or df.empty:
                return None

            # Limit to reasonable number of rows for display
            display_df = df.head(100)

            fig = go.Figure(data=[go.Table(
                header=dict(
                    values=list(display_df.columns),
                    fill_color='lightblue',
                    align='left',
                    font=dict(color='white', size=12)
                ),
                cells=dict(
                    values=[display_df[col] for col in display_df.columns],
                    fill_color='lightcyan',
                    align='left',
                    font=dict(color='black', size=11)
                )
            )])

            fig.update_layout(
                title=title,
                height=min(600, max(300, len(display_df) * 25 + 100))
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating data summary table: {str(e)}")
            return None

    def auto_visualize(self, df, query_description="Query Results"):
        """Automatically choose the best visualization for the given DataFrame"""
        try:
            if df is None or df.empty:
                logger.warning("No data provided for visualization")
                return None

            # Detect best visualization type based on columns
            columns = df.columns.tolist()

            # Status distribution
            if 'search_status' in columns and 'count' in columns:
                return px.pie(df, values='count', names='search_status',
                              title=f'Status Distribution - {query_description}')

            # Company data
            elif 'comp_name' in columns and any(col in columns for col in ['count', 'order_count']):
                count_col = 'count' if 'count' in columns else 'order_count'
                fig = px.bar(df, x='comp_name', y=count_col,
                             title=f'Company Distribution - {query_description}')
                fig.update_layout(xaxis_tickangle=-45)
                return fig

            # Search type data
            elif 'search_type_code' in columns and 'count' in columns:
                return px.bar(df, x='search_type_code', y='count',
                              title=f'Search Type Distribution - {query_description}')

            # Geographic data
            elif 'state_code' in columns and 'count' in columns:
                return px.choropleth(df, locations='state_code', locationmode="USA-states",
                                     color='count', scope="usa",
                                     title=f'Geographic Distribution - {query_description}')

            # Subject data with names
            elif 'subject_name' in columns:
                return self.create_data_summary_table(df, f'Subject Information - {query_description}')

            # Any data with count column
            elif 'count' in columns and len(df) <= 20:
                first_col = [col for col in columns if col != 'count'][0]
                return px.bar(df, x=first_col, y='count',
                              title=f'Distribution - {query_description}')

            # Default to table for complex data
            else:
                return self.create_data_summary_table(df, query_description)

        except Exception as e:
            logger.error(f"Error in auto visualization: {str(e)}")
            return self.create_data_summary_table(df, query_description)

    def get_table_stats(self, table_name):
        """Get basic statistics for a table"""
        try:
            if not self.check_table_exists(table_name):
                return None

            query = f'SELECT COUNT(*) as total_rows FROM "{table_name}"'
            df = self.execute_query(query)

            if not df.empty:
                return {"total_rows": df.iloc[0]['total_rows']}
            return None

        except Exception as e:
            logger.error(f"Error getting table stats: {str(e)}")
            return None