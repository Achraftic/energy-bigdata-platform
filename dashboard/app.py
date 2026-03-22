import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# API configuration
API_URL = "http://api:8000"

# Fetch data from API
def fetch_data(endpoint, region=None):
    try:
        params = {"region": region} if region else {}
        response = requests.get(f"{API_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from {endpoint}: {e}")
        return pd.DataFrame()


def fetch_regions():
    try:
        response = requests.get(f"{API_URL}/regions")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error fetching regions: {e}")
        return []


def main():
    st.set_page_config(page_title="Energy Data Dashboard", layout="wide")
    st.title("⚡ Energy Data & Analytics Dashboard")

    # Sidebar Navigation & Filtering
    st.sidebar.header("Navigation & Filters")
    page = st.sidebar.selectbox("Choose View", ["Overview", "Deeper Analytics", "Anomalies", "Forecast", "Rapports & Stats"])
    
    # Get regions from backend
    all_regions = fetch_regions()
    selected_region = st.sidebar.selectbox("Select Region", ["All Regions"] + all_regions)
    
    region_filter = None if selected_region == "All Regions" else selected_region

    # Fetch filtered data
    metrics_df = fetch_data("metrics", region=region_filter)
    anomalies_df = fetch_data("anomalies", region=region_filter)
    forecast_df = fetch_data("forecast", region=region_filter)
    summary_df = fetch_data("stats/summary") # Global stats for comparison

    # Convert timestamps
    if not metrics_df.empty:
        metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'])
    
    if not anomalies_df.empty:
        anomalies_df['timestamp'] = pd.to_datetime(anomalies_df['timestamp'])

    if not forecast_df.empty:
        forecast_df['timestamp'] = pd.to_datetime(forecast_df['timestamp'])

    if page == "Overview":
        st.header(f"Unified Energy Metrics: {selected_region}")
        
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

            if selected_region == "All Regions":
                st.subheader("Regional Performance Heatmap")
                # Pivoting for heatmap
                heatmap_df = metrics_df.pivot_table(
                    index='region', 
                    columns=metrics_df['timestamp'].dt.hour, 
                    values='production', 
                    aggfunc='mean'
                )
                fig_heat = px.imshow(
                    heatmap_df,
                    labels=dict(x="Hour of Day", y="Region", color="Avg Production"),
                    x=heatmap_df.columns,
                    y=heatmap_df.index,
                    title="Average Production Heatmap (Hourly)",
                    template="plotly_dark",
                    color_continuous_scale="Viridis"
                )
                st.plotly_chart(fig_heat, use_container_width=True)
            else:
                st.subheader(f"Historical Trends for {selected_region}")
                chart_df = metrics_df.melt(
                    id_vars=["timestamp"],
                    value_vars=["consumption", "production"],
                    var_name="metric",
                    value_name="MW",
                )
                fig_line = px.line(
                    chart_df,
                    x="timestamp",
                    y="MW",
                    color="metric",
                    title=f"Consumption vs Production in {selected_region}",
                    template="plotly_dark"
                )
                st.plotly_chart(fig_line, use_container_width=True)

    elif page == "Deeper Analytics":
        st.header(f"Real-World Energy Analytics: {selected_region}")
        
        if not summary_df.empty:
            st.markdown("### 📈 Regional Competitive Benchmarking")
            # Ranking logic
            summary_df['rank'] = summary_df['efficiency_ratio'].rank(ascending=False).astype(int)
            total_regions = len(summary_df)
            
            if region_filter:
                reg_data = summary_df[summary_df['region'] == region_filter].iloc[0]
                col_r1, col_r2, col_r3 = st.columns(3)
                with col_r1:
                    st.metric("National Efficiency Rank", f"#{reg_data['rank']} / {total_regions}", 
                              delta="Top Tier" if reg_data['rank'] <= 3 else None)
                with col_r2:
                    st.metric("Avg Grid Voltage", f"{reg_data['avg_voltage']:.1f}V", 
                              delta=f"{reg_data['avg_voltage'] - 230:.2f}V from target")
                with col_r3:
                    st.metric("System Efficiency", f"{reg_data['efficiency_ratio']:.1%}")
            
            fig_bench = px.bar(
                summary_df.sort_values('efficiency_ratio', ascending=False), 
                x='region', y='efficiency_ratio', color='efficiency_ratio',
                title="National Efficiency Comparison (Production/Consumption Ratio)", 
                template="plotly_dark", color_continuous_scale="RdYlGn"
            )
            if region_filter:
                fig_bench.add_annotation(x=region_filter, text="SELECTED", showarrow=True, arrowhead=1, bgcolor="white")
            st.plotly_chart(fig_bench, use_container_width=True)

        if not metrics_df.empty:
            st.divider()
            col_a, col_b = st.columns(2)
            
            with col_a:
                st.markdown("### ⚖️ Net Energy Balance (Supply vs Demand)")
                metrics_df['net_balance'] = metrics_df['production'] - metrics_df['consumption']
                fig_bal = px.area(
                    metrics_df.sort_values('timestamp'), x='timestamp', y='net_balance',
                    title="Grid Surplus/Deficit Over Time (MW)",
                    color_discrete_sequence=['#00CC96'], # Green for surplus
                    template="plotly_dark"
                )
                fig_bal.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="Deficit Threshold")
                st.plotly_chart(fig_bal, use_container_width=True)
            
            with col_b:
                st.markdown("### 🛡️ Grid Stability Matrix")
                fig_stab = px.scatter(
                    metrics_df, x='voltage', y='frequency', color='equipment_status',
                    title="Voltage vs Frequency Correlation (Power Quality)",
                    labels={'voltage': 'Voltage (V)', 'frequency': 'Frequency (Hz)'},
                    template="plotly_dark",
                    marginal_x="histogram", marginal_y="histogram"
                )
                # Add safe zones
                fig_stab.add_vline(x=230, line_dash="dot", line_color="rgba(255,255,255,0.5)")
                fig_stab.add_hline(y=50, line_dash="dot", line_color="rgba(255,255,255,0.5)")
                st.plotly_chart(fig_stab, use_container_width=True)

            st.divider()
            st.markdown("### 🏭 Load Composition & Operational Health")
            col_c, col_d = st.columns(2)
            
            with col_c:
                cons_type = metrics_df.groupby('consumer_type')['consumption'].sum().reset_index()
                fig_cons = px.pie(cons_type, values='consumption', names='consumer_type', 
                                 title="Total Energy Consumption by Sector", hole=0.6, 
                                 template="plotly_dark", color_discrete_sequence=px.colors.sequential.Electric)
                st.plotly_chart(fig_cons, use_container_width=True)
            
            with col_d:
                status_counts = metrics_df['equipment_status'].value_counts().reset_index()
                status_counts.columns = ['status', 'count']
                fig_status = px.funnel(status_counts, x='count', y='status', 
                                     title="Operational Equipment Summary", 
                                     template="plotly_dark", color='status')
                st.plotly_chart(fig_status, use_container_width=True)

    elif page == "Anomalies":
        st.header("⚠️ Anomaly Detection")
        if not anomalies_df.empty:
            st.info(f"Found {len(anomalies_df)} anomalies in {selected_region}.")
            st.dataframe(anomalies_df, use_container_width=True)
        else:
            st.success("No anomalies detected for the current selection.")

    elif page == "Forecast":
        st.header("🔮 Consumption Forecasting")
        if not forecast_df.empty:
            
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

    elif page == "Rapports & Stats":
        st.header("📊 Detailed Performance Metrics & Reports")
        
        if not summary_df.empty:
            st.subheader("Global Regional Summary (Averages)")
            # Formatting the dataframe for display
            display_df = summary_df.copy()
            display_df['avg_production'] = display_df['avg_production'].map('{:,.2f} MW'.format)
            display_df['avg_consumption'] = display_df['avg_consumption'].map('{:,.2f} MW'.format)
            display_df['avg_voltage'] = display_df['avg_voltage'].map('{:,.2f} V'.format)
            display_df['efficiency_ratio'] = display_df['efficiency_ratio'].map('{:.2%}'.format)
            
            st.dataframe(display_df, use_container_width=True)
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="Exporter le Résumé (CSV)",
                    data=summary_df.to_csv(index=False),
                    file_name="energy_summary.csv",
                    mime="text/csv"
                )
            with col2:
                if not metrics_df.empty:
                    st.download_button(
                        label="Exporter les Données Brutes (CSV)",
                        data=metrics_df.to_csv(index=False),
                        file_name=f"energy_data_{selected_region}.csv",
                        mime="text/csv"
                    )
            
            st.subheader("Analyse de la Fréquence Moyenne")
            if not metrics_df.empty:
                avg_freq = metrics_df.groupby('region')['frequency'].mean().reset_index()
                st.table(avg_freq)
        else:
            st.info("Chargement des rapports...")

if __name__ == "__main__":
    main()
