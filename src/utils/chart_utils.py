import pandas as pd
import plotly.express as px


def detect_visualization_type(df):
    """Auto-detect the best visualization type based on query results"""
    if isinstance(df, pd.DataFrame) and not df.empty:
        if len(df) == 1:
            return "single_value"
        elif 'count' in df.columns and 'Status' in df.columns:
            return "pie_chart"
        elif 'count' in df.columns and len(df) > 5:
            return "bar_chart"
        elif 'state_code' in df.columns:
            return "map"
        elif 'package_price' in df.columns:
            return "scatter"
    return "table"


def create_auto_visualization(df, title):
    """Create automatic visualization based on data"""
    viz_type = detect_visualization_type(df)

    if viz_type == "pie_chart" and 'Status' in df.columns and 'count' in df.columns:
        return px.pie(df, values='count', names='Status', title=title)
    elif viz_type == "bar_chart" and 'count' in df.columns:
        x_col = df.columns[0] if len(df.columns) > 1 else 'category'
        return px.bar(df, x=x_col, y='count', title=title)
    elif viz_type == "map" and 'state_code' in df.columns and 'count' in df.columns:
        return px.choropleth(df, locations='state_code', locationmode="USA-states",
                             color='count', scope="usa", title=title)
    else:
        return None