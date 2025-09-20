import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import logging
from typing import Optional, Dict, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Color schemes for different chart types
COLOR_SCHEMES = {
    'status': ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3'],
    'company': ['#3498DB', '#E74C3C', '#2ECC71', '#F39C12', '#9B59B6', '#1ABC9C'],
    'geographic': 'Blues',
    'default': ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
}


def detect_visualization_type(df: pd.DataFrame) -> str:
    """Auto-detect the best visualization type based on DataFrame structure"""
    try:
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.warning("Invalid or empty DataFrame provided")
            return "none"

        cols = df.columns.tolist()
        num_rows = len(df)
        num_cols = len(cols)

        logger.info(f"Analyzing DataFrame: {num_rows} rows, {num_cols} columns")

        # Single value result (metric display)
        if num_rows == 1 and num_cols == 1:
            return "metric"

        # Single value with description
        if num_rows == 1 and num_cols == 2:
            return "metric_with_label"

        # Status distribution patterns
        if any('status' in col.lower() for col in cols) and 'count' in cols:
            return "status_pie"

        # Company-related data
        elif any('comp' in col.lower() for col in cols):
            if 'count' in cols or any('order' in col.lower() for col in cols):
                return "company_bar"
            else:
                return "company_table"

        # Geographic data
        elif 'state_code' in cols and 'count' in cols:
            return "geo_map"

        # Search type data
        elif any('type' in col.lower() for col in cols) and 'count' in cols:
            return "type_bar"

        # Price/financial data
        elif any('price' in col.lower() or 'cost' in col.lower() for col in cols):
            return "price_scatter"

        # Time series (if date columns exist)
        elif any('date' in col.lower() or 'time' in col.lower() for col in cols):
            return "timeline"

        # Subject/person data
        elif any('subject' in col.lower() or 'name' in col.lower() for col in cols):
            return "subject_table"

        # Categorical data with counts (good for bar charts)
        elif 'count' in cols and num_rows <= 25:
            return "category_bar"

        # Large datasets (table view)
        elif num_rows > 100:
            return "large_table"

        # Medium datasets with multiple columns
        elif num_rows > 20 and num_cols > 3:
            return "data_table"

        # Small datasets
        else:
            return "simple_table"

    except Exception as e:
        logger.error(f"Error detecting visualization type: {str(e)}")
        return "table"


def create_auto_visualization(df: pd.DataFrame, title: str = "Data Visualization") -> Optional[go.Figure]:
    """Create appropriate visualization based on data characteristics"""
    try:
        if df is None or df.empty:
            logger.warning("No data provided for visualization")
            return None

        viz_type = detect_visualization_type(df)
        logger.info(f"Creating visualization of type: {viz_type}")

        # Route to appropriate visualization function
        viz_functions = {
            "status_pie": create_status_pie_chart,
            "company_bar": create_company_bar_chart,
            "geo_map": create_geo_map,
            "type_bar": create_type_bar_chart,
            "category_bar": create_generic_bar_chart,
            "price_scatter": create_price_scatter,
            "timeline": create_timeline,
            "metric": create_metric_display,
            "metric_with_label": create_metric_with_label,
            "subject_table": create_subject_table,
            "company_table": create_company_table,
            "large_table": create_large_table,
            "data_table": create_data_table,
            "simple_table": create_simple_table
        }

        viz_function = viz_functions.get(viz_type, create_data_table)
        return viz_function(df, title)

    except Exception as e:
        logger.error(f"Error creating auto visualization: {str(e)}")
        return create_fallback_table(df, title)


def create_status_pie_chart(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create pie chart for status distribution"""
    try:
        status_col = next((col for col in df.columns if 'status' in col.lower()), df.columns[0])
        count_col = 'count' if 'count' in df.columns else df.columns[-1]

        fig = px.pie(df, values=count_col, names=status_col,
                     title=title,
                     color_discrete_sequence=COLOR_SCHEMES['status'])

        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
        )

        fig.update_layout(
            showlegend=True,
            font=dict(size=12),
            height=500
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating status pie chart: {str(e)}")
        return None


def create_company_bar_chart(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create bar chart for company data"""
    try:
        comp_col = next((col for col in df.columns if 'comp' in col.lower()), df.columns[0])
        count_col = next((col for col in df.columns if 'count' in col.lower() or 'order' in col.lower()),
                         df.columns[-1])

        # Limit to top entries for readability
        if len(df) > 15:
            plot_df = df.nlargest(15, count_col)
            title += " (Top 15)"
        else:
            plot_df = df

        fig = px.bar(plot_df, x=comp_col, y=count_col,
                     title=title,
                     color=count_col,
                     color_continuous_scale='Blues')

        fig.update_layout(
            xaxis_tickangle=-45,
            xaxis_title=comp_col.replace('_', ' ').title(),
            yaxis_title=count_col.replace('_', ' ').title(),
            height=600,
            margin=dict(b=100)
        )

        fig.update_traces(
            hovertemplate='<b>%{x}</b><br>%{y}<extra></extra>'
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating company bar chart: {str(e)}")
        return None


def create_geo_map(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create geographical choropleth map"""
    try:
        state_col = 'state_code'
        count_col = 'count'

        if state_col not in df.columns or count_col not in df.columns:
            logger.warning("Required columns for geo map not found")
            return None

        # Filter out invalid state codes
        valid_df = df[df[state_col].str.len() == 2].copy()

        if valid_df.empty:
            logger.warning("No valid state codes found for mapping")
            return None

        fig = px.choropleth(valid_df,
                            locations=state_col,
                            locationmode="USA-states",
                            color=count_col,
                            scope="usa",
                            title=title,
                            color_continuous_scale=COLOR_SCHEMES['geographic'],
                            labels={count_col: 'Count', state_col: 'State'})

        fig.update_layout(
            geo=dict(
                showlakes=True,
                lakecolor='rgb(255, 255, 255)',
                showsubunits=True
            ),
            height=500
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating geo map: {str(e)}")
        return None


def create_type_bar_chart(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create bar chart for search types"""
    try:
        type_col = next((col for col in df.columns if 'type' in col.lower()), df.columns[0])
        count_col = 'count' if 'count' in df.columns else df.columns[-1]

        # Limit for readability
        if len(df) > 12:
            plot_df = df.nlargest(12, count_col)
            title += " (Top 12)"
        else:
            plot_df = df

        fig = px.bar(plot_df, x=type_col, y=count_col,
                     title=title,
                     color=count_col,
                     color_continuous_scale='Viridis')

        fig.update_layout(
            xaxis_tickangle=-45,
            height=500,
            xaxis_title=type_col.replace('_', ' ').title(),
            yaxis_title='Count'
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating type bar chart: {str(e)}")
        return None


def create_generic_bar_chart(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create generic bar chart for categorical data"""
    try:
        if 'count' in df.columns:
            x_col = [col for col in df.columns if col != 'count'][0]
            y_col = 'count'
        else:
            x_col = df.columns[0]
            y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

        fig = px.bar(df, x=x_col, y=y_col, title=title,
                     color_discrete_sequence=COLOR_SCHEMES['default'])

        if len(df) > 10:
            fig.update_layout(xaxis_tickangle=-45)

        fig.update_layout(
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title(),
            height=500
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating generic bar chart: {str(e)}")
        return None


def create_price_scatter(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create scatter plot for price/financial data"""
    try:
        price_cols = [col for col in df.columns if 'price' in col.lower() or 'cost' in col.lower()]
        if not price_cols:
            return None

        price_col = price_cols[0]

        # Try to find a suitable y-axis (usage, count, etc.)
        y_candidates = ['usage', 'count', 'quantity', 'orders']
        y_col = None
        for candidate in y_candidates:
            matching_cols = [col for col in df.columns if candidate in col.lower()]
            if matching_cols:
                y_col = matching_cols[0]
                break

        if not y_col:
            y_col = df.columns[-1] if len(df.columns) > 1 else price_col

        # Color by category if available
        color_col = None
        for col in df.columns:
            if 'category' in col.lower() or 'type' in col.lower() or 'name' in col.lower():
                if df[col].nunique() < 10:  # Reasonable number of categories
                    color_col = col
                    break

        if color_col:
            fig = px.scatter(df, x=price_col, y=y_col, color=color_col,
                             title=title, size=y_col if y_col != price_col else None)
        else:
            fig = px.scatter(df, x=price_col, y=y_col, title=title)

        fig.update_layout(
            xaxis_title=price_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title(),
            height=500
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating price scatter plot: {str(e)}")
        return None


def create_timeline(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create timeline visualization"""
    try:
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if not date_cols:
            return None

        date_col = date_cols[0]

        # Convert to datetime if not already
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df_clean = df.dropna(subset=[date_col])

        if df_clean.empty:
            return None

        # Group by date and count
        if len(df.columns) > 1:
            value_col = [col for col in df.columns if col != date_col][0]
            if pd.api.types.is_numeric_dtype(df[value_col]):
                daily_data = df_clean.groupby(df_clean[date_col].dt.date)[value_col].sum().reset_index()
            else:
                daily_data = df_clean.groupby(df_clean[date_col].dt.date).size().reset_index(name='count')
                value_col = 'count'
        else:
            daily_data = df_clean.groupby(df_clean[date_col].dt.date).size().reset_index(name='count')
            value_col = 'count'

        fig = px.line(daily_data, x=date_col, y=value_col, title=title)
        fig.update_traces(mode='lines+markers')

        return fig
    except Exception as e:
        logger.error(f"Error creating timeline: {str(e)}")
        return None


def create_metric_display(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create metric display for single values"""
    try:
        value = df.iloc[0, 0]
        metric_name = df.columns[0].replace('_', ' ').title()

        # Format large numbers
        if isinstance(value, (int, float)):
            if value >= 1000000:
                display_value = f"{value / 1000000:.1f}M"
            elif value >= 1000:
                display_value = f"{value / 1000:.1f}K"
            else:
                display_value = f"{value:,.0f}"
        else:
            display_value = str(value)

        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode="number",
            value=value,
            title={"text": f"{title}<br><span style='font-size:0.8em'>{metric_name}</span>"},
            number={
                'font': {'size': 60, 'color': '#1f77b4'},
                'suffix': ""
            },
            domain={'x': [0, 1], 'y': [0, 1]}
        ))

        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=80, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating metric display: {str(e)}")
        return None


def create_metric_with_label(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create metric display with label for two-column data"""
    try:
        value = df.iloc[0, 0]
        label = df.iloc[0, 1] if len(df.columns) > 1 else df.columns[0]

        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode="number",
            value=value,
            title={"text": f"{title}<br><span style='font-size:0.7em'>{label}</span>"},
            number={'font': {'size': 50, 'color': '#2E86AB'}},
        ))

        fig.update_layout(height=250, margin=dict(l=20, r=20, t=60, b=20))
        return fig
    except Exception as e:
        logger.error(f"Error creating metric with label: {str(e)}")
        return None


def create_subject_table(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create formatted table for subject/person data"""
    try:
        # Limit to reasonable number of rows
        display_df = df.head(50) if len(df) > 50 else df

        # Mask sensitive information if present
        masked_df = display_df.copy()
        sensitive_cols = ['contact', 'phone', 'email', 'address']

        for col in masked_df.columns:
            if any(sensitive in col.lower() for sensitive in sensitive_cols):
                masked_df[col] = masked_df[col].astype(str).str[:3] + '***'

        fig = go.Figure(data=[go.Table(
            header=dict(
                values=[col.replace('_', ' ').title() for col in masked_df.columns],
                fill_color='lightsteelblue',
                align='left',
                font=dict(color='white', size=12, family="Arial"),
                height=40
            ),
            cells=dict(
                values=[masked_df[col] for col in masked_df.columns],
                fill_color=[['lightcyan', 'white'] * len(masked_df)],
                align='left',
                font=dict(color='black', size=11),
                height=30
            )
        )])

        fig.update_layout(
            title=title,
            height=min(800, max(300, len(display_df) * 35 + 150)),
            margin=dict(l=10, r=10, t=60, b=10)
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating subject table: {str(e)}")
        return None


def create_company_table(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create formatted table for company data"""
    try:
        display_df = df.head(100) if len(df) > 100 else df

        fig = go.Figure(data=[go.Table(
            header=dict(
                values=[col.replace('_', ' ').title() for col in display_df.columns],
                fill_color='darkblue',
                align='left',
                font=dict(color='white', size=12),
                height=40
            ),
            cells=dict(
                values=[display_df[col] for col in display_df.columns],
                fill_color='lightblue',
                align='left',
                font=dict(color='black', size=11),
                height=30
            )
        )])

        fig.update_layout(
            title=title,
            height=min(700, max(300, len(display_df) * 35 + 120)),
            margin=dict(l=10, r=10, t=60, b=10)
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating company table: {str(e)}")
        return None


def create_large_table(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create paginated table for large datasets"""
    try:
        # Show only first 100 rows for performance
        display_df = df.head(100)

        fig = go.Figure(data=[go.Table(
            header=dict(
                values=[col.replace('_', ' ').title() for col in display_df.columns],
                fill_color='navy',
                align='left',
                font=dict(color='white', size=11),
                height=35
            ),
            cells=dict(
                values=[display_df[col] for col in display_df.columns],
                fill_color=[['lightgray', 'white'] * len(display_df)],
                align='left',
                font=dict(color='black', size=10),
                height=25
            )
        )])

        total_rows = len(df)
        shown_rows = len(display_df)
        table_title = f"{title}<br><sub>Showing {shown_rows} of {total_rows} rows</sub>"

        fig.update_layout(
            title=table_title,
            height=600,
            margin=dict(l=10, r=10, t=80, b=10)
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating large table: {str(e)}")
        return None


def create_data_table(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create standard data table"""
    try:
        display_df = df.head(50) if len(df) > 50 else df

        fig = go.Figure(data=[go.Table(
            header=dict(
                values=[col.replace('_', ' ').title() for col in display_df.columns],
                fill_color='lightblue',
                align='left',
                font=dict(color='white', size=12),
                height=35
            ),
            cells=dict(
                values=[display_df[col] for col in display_df.columns],
                fill_color='lightcyan',
                align='left',
                font=dict(color='black', size=11),
                height=28
            )
        )])

        fig.update_layout(
            title=title,
            height=min(600, max(250, len(display_df) * 30 + 100)),
            margin=dict(l=10, r=10, t=60, b=10)
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating data table: {str(e)}")
        return None


def create_simple_table(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create simple table for small datasets"""
    try:
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=[col.replace('_', ' ').title() for col in df.columns],
                fill_color='steelblue',
                align='center',
                font=dict(color='white', size=12, family="Arial Black"),
                height=40
            ),
            cells=dict(
                values=[df[col] for col in df.columns],
                fill_color='aliceblue',
                align='center',
                font=dict(color='black', size=11),
                height=32
            )
        )])

        fig.update_layout(
            title=title,
            height=min(400, len(df) * 35 + 120),
            margin=dict(l=20, r=20, t=60, b=20)
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating simple table: {str(e)}")
        return None


def create_fallback_table(df: pd.DataFrame, title: str) -> Optional[go.Figure]:
    """Create basic fallback table when other visualizations fail"""
    try:
        # Very simple table as last resort
        display_df = df.head(20) if len(df) > 20 else df

        fig = go.Figure(data=[go.Table(
            header=dict(values=list(display_df.columns), fill_color='gray'),
            cells=dict(values=[display_df[col] for col in display_df.columns], fill_color='lightgray')
        )])

        fig.update_layout(title=title, height=400)
        return fig
    except Exception as e:
        logger.error(f"Error creating fallback table: {str(e)}")
        return None


def create_multi_chart_dashboard(data_dict: Dict[str, pd.DataFrame], main_title: str = "Analytics Dashboard") -> \
Optional[go.Figure]:
    """Create a dashboard with multiple charts"""
    try:
        num_charts = len(data_dict)
        if num_charts == 0:
            return None

        # Determine subplot layout
        if num_charts == 1:
            rows, cols = 1, 1
        elif num_charts == 2:
            rows, cols = 1, 2
        elif num_charts <= 4:
            rows, cols = 2, 2
        else:
            rows, cols = 3, 2  # Limit to 6 charts max

        fig = make_subplots(
            rows=rows, cols=cols,
            subplot_titles=list(data_dict.keys())[:6],  # Limit titles
            specs=[[{"type": "xy"}] * cols for _ in range(rows)]
        )

        chart_idx = 0
        for chart_title, df in list(data_dict.items())[:6]:  # Limit to 6 charts
            row = (chart_idx // cols) + 1
            col = (chart_idx % cols) + 1

            # Create simple bar chart for each dataset
            if not df.empty and len(df.columns) >= 2:
                x_col, y_col = df.columns[0], df.columns[1]
                fig.add_trace(
                    go.Bar(x=df[x_col].head(10), y=df[y_col].head(10), name=chart_title),
                    row=row, col=col
                )

            chart_idx += 1

        fig.update_layout(
            height=800,
            title_text=main_title,
            showlegend=False
        )

        return fig
    except Exception as e:
        logger.error(f"Error creating multi-chart dashboard: {str(e)}")
        return None


def enhance_chart_styling(fig: go.Figure, chart_type: str = "default") -> go.Figure:
    """Apply consistent styling to charts"""
    try:
        # Common styling for all charts
        fig.update_layout(
            font=dict(family="Arial, sans-serif", size=12),
            title_font_size=16,
            title_font_color='#2E86AB',
            paper_bgcolor='white',
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=50)
        )

        # Chart-specific styling
        if chart_type == "bar":
            fig.update_traces(
                marker_color='lightblue',
                marker_line_color='darkblue',
                marker_line_width=1
            )
            fig.update_layout(
                xaxis=dict(showgrid=True, gridcolor='lightgray'),
                yaxis=dict(showgrid=True, gridcolor='lightgray')
            )
        elif chart_type == "pie":
            fig.update_traces(
                marker=dict(line=dict(color='white', width=2)),
                textfont_size=12
            )
        elif chart_type == "scatter":
            fig.update_traces(
                marker=dict(size=8, line=dict(width=1, color='darkblue'))
            )
        elif chart_type == "line":
            fig.update_traces(
                line=dict(width=3),
                marker=dict(size=6)
            )

        return fig
    except Exception as e:
        logger.error(f"Error enhancing chart styling: {str(e)}")
        return fig


def create_summary_stats_card(df: pd.DataFrame) -> Dict[str, any]:
    """Create summary statistics for a DataFrame"""
    try:
        stats = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': len(df.select_dtypes(include=['object']).columns),
            'duplicate_rows': df.duplicated().sum()
        }
        return stats
    except Exception as e:
        logger.error(f"Error creating summary stats: {str(e)}")
        return {}


def export_chart_as_image(fig: go.Figure, filename: str, format: str = 'png', width: int = 1200, height: int = 800):
    """Export chart as image file"""
    try:
        fig.write_image(filename, format=format, width=width, height=height)
        logger.info(f"Chart exported as {filename}")
        return True
    except Exception as e:
        logger.error(f"Error exporting chart: {str(e)}")
        return False