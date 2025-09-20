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
        self.engine = engine

    def check_table_exists(self, table_name):
        """Check if a table exists in the database"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def execute_query(self, query):
        """Execute SQL query and return DataFrame"""
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return pd.DataFrame()

    def create_status_pie_chart(self):
        """Create pie chart of search status distribution"""
        if not self.check_table_exists('Search Table') or not self.check_table_exists('Search_status'):
            logger.warning("Required tables (Search Table or Search_status) not found")
            return None

        query = """
        SELECT ss.status, COUNT(*) as count 
        FROM "Search Table" s
        JOIN "Search_status" ss ON s.search_status = ss.status_code
        GROUP BY ss.status
        """
        df = self.execute_query(text(query))

        if not df.empty:
            fig = px.pie(df, values='count', names='status',
                         title='Background Check Status Distribution')
            return fig
        return None

    def create_company_bar_chart(self):
        """Create bar chart of orders by company"""
        if not self.check_table_exists('Order_Request Table') or not self.check_table_exists('Company Table'):
            logger.warning("Required tables (Order_Request Table or Company Table) not found")
            return None

        query = """
        SELECT c.comp_name, COUNT(*) as order_count
        FROM "Order_Request Table" o
        JOIN "Company Table" c ON o.order_companycode = c.comp_code
        GROUP BY c.comp_name
        ORDER BY order_count DESC
        LIMIT 10
        """
        df = self.execute_query(text(query))

        if not df.empty:
            fig = px.bar(df, x='comp_name', y='order_count',
                         title='Top 10 Companies by Number of Orders',
                         labels={'comp_name': 'Company', 'order_count': 'Number of Orders'})
            fig.update_layout(xaxis_tickangle=-45)
            return fig
        return None

    def create_search_type_treemap(self):
        """Create treemap of search types by category"""
        if not self.check_table_exists('Search Table') or not self.check_table_exists('Search_Type Table'):
            logger.warning("Required tables (Search Table or Search_Type Table) not found")
            return None

        query = """
        SELECT st.search_type_category, st.search_type, COUNT(*) as count
        FROM "Search Table" s
        JOIN "Search_Type Table" st ON s.search_type_code = st.search_type_code
        GROUP BY st.search_type_category, st.search_type
        """
        df = self.execute_query(text(query))

        if not df.empty:
            fig = px.treemap(df, path=['search_type_category', 'search_type'], values='count',
                             title='Search Types Distribution by Category')
            return fig
        return None

    def create_geographical_map(self):
        """Create geographical distribution map"""
        if not self.check_table_exists('Search Table'):
            logger.warning("Required table (Search Table) not found")
            return None

        query = """
        SELECT state_code, COUNT(*) as count
        FROM "Search Table"
        WHERE state_code IS NOT NULL AND state_code NOT IN ('NA', 'I', 'NONE')
        GROUP BY state_code
        """
        df = self.execute_query(text(query))

        if not df.empty:
            fig = px.choropleth(df, locations='state_code', locationmode="USA-states",
                                color='count', scope="usa",
                                title='Background Checks by State',
                                labels={'count': 'Number of Checks'})
            return fig
        return None

    def create_package_price_analysis(self):
        """Analyze package prices"""
        if not self.check_table_exists('Package Table') or not self.check_table_exists('Company Table') or not self.check_table_exists('Order_Request Table'):
            logger.warning("Required tables (Package Table, Company Table, or Order_Request Table) not found")
            return None

        query = """
        SELECT p.package_name, p.package_price, c.comp_name, COUNT(o.order_id) as usage_count
        FROM "Package Table" p
        JOIN "Company Table" c ON p.comp_code = c.comp_code
        LEFT JOIN "Order_Request Table" o ON p.package_code = o.order_packagecode
        GROUP BY p.package_name, p.package_price, c.comp_name
        """
        df = self.execute_query(text(query))

        if not df.empty:
            fig = px.scatter(df, x='package_price', y='usage_count', size='usage_count',
                             color='comp_name', hover_name='package_name',
                             title='Package Price vs Usage Analysis',
                             labels={'package_price': 'Package Price ($)', 'usage_count': 'Number of Orders'})
            return fig
        return None