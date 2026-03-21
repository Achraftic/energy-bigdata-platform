import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# API configuration
API_URL = "http://api:8000"

# Fetch data from API
def fetch_data(endpoint):
    try:
        response = requests.get(f"{API_URL}/{endpoint}")
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from {endpoint}: {e}")
        return pd.DataFrame()


def main():
    st.set_page_config(page_title="Energy Data Dashboard", layout="wide")
    st.title("⚡ Energy Data & Analytics Dashboard")

    # Fetch data
    metrics_df = fetch_data("metrics")
    anomalies_df = fetch_data("anomalies")
    forecast_df = fetch_data("forecast")

    # Sidebar Navigation & Filtering
    st.sidebar.header("Navigation & Filters")
    page = st.sidebar.selectbox("Choose View", ["Overview", "Deeper Analytics", "Anomalies", "Forecast"])
    
    all_regions = []
    if not metrics_df.empty:
        all_regions = sorted(metrics_df['region'].unique())
    
    selected_regions = st.sidebar.multiselect("Filter by Region", options=all_regions, default=all_regions)

    # Filter dataframes
    if not metrics_df.empty:
        metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'])
        metrics_df = metrics_df[metrics_df['region'].isin(selected_regions)]
    
    if not anomalies_df.empty:
        anomalies_df['timestamp'] = pd.to_datetime(anomalies_df['timestamp'])
        anomalies_df = anomalies_df[anomalies_df['region'].isin(selected_regions)]

    if page == "Overview":
        st.header("Unified Energy Metrics")
        
        if not metrics_df.empty:
            # Summary Cards
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Avg Production (MW)", f"{metrics_df['production'].mean():.2f}")
            with col2:
                st.metric("Avg Consumption (MW)", f"{metrics_df['consumption'].mean():.2f}")
            with col3:
                st.metric("Avg Frequency (Hz)", f"{metrics_df['frequency'].mean():.2f}")
            with col4:
                st.metric("Total Records", len(metrics_df))

            # Main Trends
            st.subheader("Historical Trends (Consumption vs Production)")
            chart_df = metrics_df.melt(
                id_vars=["timestamp", "region"],
                value_vars=["consumption", "production"],
                var_name="metric",
                value_name="MW",
            )
            fig_line = px.line(
                chart_df,
                x="timestamp",
                y="MW",
                color="metric",
                line_group="region",
                hover_name="region",
                title="Cons. vs Prod. Over Time",
                template="plotly_dark"
            )
            st.plotly_chart(fig_line, use_container_width=True)

    elif page == "Deeper Analytics":
        st.header("Regional & Equipment Deep Dive")
        
        if not metrics_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Total Production by Region")
                prod_by_region = metrics_df.groupby('region')['production'].sum().reset_index()
                fig_bar = px.bar(prod_by_region, x='region', y='production', color='region', 
                                title="Prod/Region", template="plotly_dark")
                st.plotly_chart(fig_bar, use_container_width=True)
            
            with col2:
                st.subheader("Equipment Status Distribution")
                status_counts = metrics_df['equipment_status'].value_counts().reset_index()
                status_counts.columns = ['status', 'count']
                fig_pie = px.pie(status_counts, values='count', names='status', 
                                title="Equip Status", template="plotly_dark")
                st.plotly_chart(fig_pie, use_container_width=True)

    elif page == "Anomalies":
        st.header("⚠️ Anomaly Detection")
        if not anomalies_df.empty:
            st.info(f"Found {len(anomalies_df)} anomalies in selected regions.")
            st.dataframe(anomalies_df, use_container_width=True)
        else:
            st.success("No anomalies detected for the current selection.")

    elif page == "Forecast":
        st.header("🔮 Consumption Forecasting")
        if not forecast_df.empty:
            forecast_df['timestamp'] = pd.to_datetime(forecast_df['timestamp'])
            forecast_df = forecast_df[forecast_df['region'].isin(selected_regions)]
            
            fig_forecast = px.line(
                forecast_df,
                x="timestamp",
                y="forecasted_consumption",
                color="region",
                title="Forecasted Consumption by Region (Next 24h)",
                template="plotly_dark"
            )
            st.plotly_chart(fig_forecast, use_container_width=True)
            st.dataframe(forecast_df, use_container_width=True)
        else:
            st.warning("No forecast data available.")

if __name__ == "__main__":
    main()
