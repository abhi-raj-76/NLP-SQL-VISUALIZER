import pandas as pd
import plotly.express as px
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def detect_visualization_type(df):
    """Auto-detect the best visualization type based on query results"""
    try:
        if isinstance(df, pd.DataFrame) and not df.empty:
            if len(df) == 1:
                return "single_value"
            elif 'count' in df.columns and 'status' in df.columns:
                return "pie_chart"
            elif 'count' in df.columns and len(df) > 5:
                return "bar_chart"
            elif 'state_code' in df.columns:
                return "map"
            elif 'package_price' in df.columns:
                return "scatter"
        logger.info("Defaulting to table visualization")
        return "table"
    except Exception as e:
        logger.error(f"Error detecting visualization type: {str(e)}")
        return "table"

def create_auto_visualization(df, title):
    """Create automatic visualization based on data"""
    try:
        viz_type = detect_visualization_type(df)

        if viz_type == "pie_chart" and 'status' in df.columns and 'count' in df.columns:
            return px.pie(df, values='count', names='status', title=title)
        elif viz_type == "bar_chart" and 'count' in df.columns:
            x_col = df.columns[0] if len(df.columns) > 1 else 'category'
            return px.bar(df, x=x_col, y='count', title=title)
        elif viz_type == "map" and 'state_code' in df.columns and 'count' in df.columns:
            return px.choropleth(df, locations='state_code', locationmode="USA-states",
                                 color='count', scope="usa", title=title)
        else:
            logger.info("No specific visualization type matched, returning None")
            return None
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        return None