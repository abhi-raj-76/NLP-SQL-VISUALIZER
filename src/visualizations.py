import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text, inspect  # ADD inspect import


class DataVisualizer:
    def __init__(self, engine):
        self.engine = engine

    def check_table_exists(self, table_name):
        """Check if a table exists in the database"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def execute_query(self, query):
        """Execute SQL query and return DataFrame"""
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def create_status_pie_chart(self):
        """Create pie chart of search status distribution"""
        # CHECK IF TABLES EXIST FIRST
        if not self.check_table_exists('search') or not self.check_table_exists('search_status'):
            return None

        query = """
        SELECT ss.Status, COUNT(*) as count 
        FROM search s
        JOIN search_status ss ON s.search_status = ss.Status_code
        GROUP BY ss.Status
        """
        df = self.execute_query(text(query))

        fig = px.pie(df, values='count', names='Status',
                     title='Background Check Status Distribution')
        return fig

    def create_company_bar_chart(self):
        """Create bar chart of orders by company"""
        # CHECK IF TABLES EXIST FIRST
        if not self.check_table_exists('order_request') or not self.check_table_exists('company'):
            return None

        query = """
        SELECT c.comp_name, COUNT(*) as order_count
        FROM order_request o
        JOIN company c ON o.order_CompanyCode = c.comp_code
        GROUP BY c.comp_name
        ORDER BY order_count DESC
        LIMIT 10
        """
        df = self.execute_query(text(query))

        fig = px.bar(df, x='comp_name', y='order_count',
                     title='Top 10 Companies by Number of Orders',
                     labels={'comp_name': 'Company', 'order_count': 'Number of Orders'})
        fig.update_layout(xaxis_tickangle=-45)
        return fig

    def create_search_type_treemap(self):
        """Create treemap of search types by category"""
        # CHECK IF TABLES EXIST FIRST
        if not self.check_table_exists('search') or not self.check_table_exists('search_type'):
            return None

        query = """
        SELECT st.search_type_category, st.search_type, COUNT(*) as count
        FROM search s
        JOIN search_type st ON s.search_type_code = st.search_type_code
        GROUP BY st.search_type_category, st.search_type
        """
        df = self.execute_query(text(query))

        fig = px.treemap(df, path=['search_type_category', 'search_type'], values='count',
                         title='Search Types Distribution by Category')
        return fig

    def create_geographical_map(self):
        """Create geographical distribution map"""
        # CHECK IF TABLES EXIST FIRST
        if not self.check_table_exists('search'):
            return None

        query = """
        SELECT state_code, COUNT(*) as count
        FROM search
        WHERE state_code != 'NA' AND state_code != 'I' AND state_code IS NOT NULL
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
        # CHECK IF TABLES EXIST FIRST
        if not self.check_table_exists('package') or not self.check_table_exists(
                'company') or not self.check_table_exists('order_request'):
            return None

        query = """
        SELECT p.package_name, p.package_price, c.comp_name, COUNT(o.order_id) as usage_count
        FROM package p
        JOIN company c ON p.comp_code = c.comp_code
        LEFT JOIN order_request o ON p.package_code = o.Order_packcageCode
        GROUP BY p.package_name, p.package_price, c.comp_name
        """
        df = self.execute_query(text(query))

        fig = px.scatter(df, x='package_price', y='usage_count', size='usage_count',
                         color='comp_name', hover_name='package_name',
                         title='Package Price vs Usage Analysis',
                         labels={'package_price': 'Package Price ($)', 'usage_count': 'Number of Orders'})
        return fig