import os
import pandas as pd
from datetime import datetime
import streamlit as st
from sqlalchemy import create_engine

# Page config
st.set_page_config(page_title="Alert System", page_icon="🚨", layout="wide")

st.title("🚨 Real-Time Alert System")

# DB connection
@st.cache_resource
def get_database_connection():
    conn_str = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'crypto_password_123')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'crypto_warehouse')}"
    return create_engine(conn_str)

@st.cache_data(ttl=60)
def generate_alerts():
    engine = get_database_connection()
    alerts = []

    # ─── Data Freshness ───
    freshness_query = "SELECT MAX(extracted_at) AS latest_extraction FROM crypto_prices_raw"
    freshness_df = pd.read_sql(freshness_query, engine)
    latest_time = pd.to_datetime(freshness_df.iloc[0]['latest_extraction'])

    hours_since = (datetime.utcnow() - latest_time).total_seconds() / 3600
    if hours_since > 2:
        alerts.append({
            "type": "Data Freshness",
            "severity": "High",
            "message": f"No data extracted in the last {hours_since:.1f} hours",
            "timestamp": datetime.utcnow()
        })

    # ─── Pipeline Failures ───
    failure_query = """
    SELECT COUNT(*) AS failed_runs
    FROM pipeline_runs
    WHERE status = 'failed' AND started_at >= NOW() - INTERVAL '1 hour'
    """
    failure_df = pd.read_sql(failure_query, engine)
    failed_count = failure_df['failed_runs'].iloc[0]

    if failed_count > 0:
        alerts.append({
            "type": "Pipeline Failure",
            "severity": "High",
            "message": f"{failed_count} pipeline run(s) failed in the last hour",
            "timestamp": datetime.utcnow()
        })

    # ─── Price Anomalies ───
    anomaly_query = """
    SELECT symbol, price_change_pct_24h
    FROM public_marts.mart_crypto_latest_prices
    WHERE ABS(price_change_pct_24h) > 20
    """
    anomaly_df = pd.read_sql(anomaly_query, engine)
    for _, row in anomaly_df.iterrows():
        alerts.append({
            "type": "Price Anomaly",
            "severity": "Medium",
            "message": f"{row['symbol']} changed {row['price_change_pct_24h']:.1f}% in 24h",
            "timestamp": datetime.utcnow()
        })

    return alerts

# Display Alerts
try:
    alerts = generate_alerts()

    if alerts:
        st.error(f"🚨 {len(alerts)} Active Alerts")
        severity_map = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}

        for alert in alerts:
            with st.expander(f"{severity_map[alert['severity']]} {alert['type']} ({alert['severity']})"):
                st.markdown(f"**Message:** {alert['message']}")
                st.markdown(f"**Time:** {alert['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")

    else:
        st.success("✅ All systems normal — no active alerts.")

except Exception as e:
    st.error("❌ Failed to load alerts.")
    st.exception(e)
