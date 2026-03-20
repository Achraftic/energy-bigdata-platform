import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# API configuration
API_URL = "http://api:8000"

# Page configuration
st.set_page_config(page_title="Energy Data Dashboard", layout="wide")


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
    st.title("Energy Data Analysis Dashboard")

    # Sidebar navigation
    page = st.sidebar.selectbox("Choose a page", ["Metrics", "Anomalies", "Forecast"])

    if page == "Metrics":
        st.header("Real-time Energy Metrics")
        metrics_df = fetch_data("metrics")
        if not metrics_df.empty:
            st.dataframe(metrics_df)

            # Consumption vs Production chart
            st.subheader("Consumption vs. Production")
            required_cols = ["timestamp", "region", "consumption", "production"]
            if all(col in metrics_df.columns for col in required_cols):
                chart_df = metrics_df.melt(
                    id_vars=["timestamp", "region"],
                    value_vars=["consumption", "production"],
                    var_name="metric",
                    value_name="value",
                )
                fig = px.line(
                    chart_df,
                    x="timestamp",
                    y="value",
                    color="metric",
                    facet_row="region",
                    title="Consumption vs. Production by Region",
                )
                st.plotly_chart(fig)
            else:
                st.warning(
                    f"Required columns missing. Expected: {required_cols}, Got: {list(metrics_df.columns)}"
                )

    elif page == "Anomalies":
        st.header("Anomaly Alerts")
        anomalies_df = fetch_data("anomalies")
        if not anomalies_df.empty:
            st.dataframe(anomalies_df)

    elif page == "Forecast":
        st.header("Power Forecast")
        forecast_df = fetch_data("forecast")
        if not forecast_df.empty:
            st.dataframe(forecast_df)

            # Forecast visualization
            st.subheader("Consumption Forecast")
            fig = px.line(
                forecast_df,
                x="timestamp",
                y="forecasted_consumption",
                color="region",
                title="Consumption Forecast by Region",
            )
            st.plotly_chart(fig)


if __name__ == "__main__":
    main()
