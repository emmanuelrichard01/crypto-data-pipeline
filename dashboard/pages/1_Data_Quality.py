import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine

# Page config
st.set_page_config(page_title="Data Quality Dashboard", page_icon="🔍", layout="wide")

# Title
st.title("🔍 Data Quality Dashboard")

# Database connection
@st.cache_resource
def get_database_connection():
    conn_str = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'crypto_password_123')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'crypto_warehouse')}"
    return create_engine(conn_str)

@st.cache_data(ttl=300)
def load_data_quality_metrics():
    engine = get_database_connection()
    query = """
    SELECT 
        symbol,
        extraction_hour,
        data_quality_score,
        total_records,
        CASE 
            WHEN data_quality_score >= 95 THEN 'Excellent'
            WHEN data_quality_score >= 80 THEN 'Good'
            WHEN data_quality_score >= 60 THEN 'Fair'
            ELSE 'Poor'
        END AS quality_category
    FROM public_marts.mart_crypto_hourly_performance
    WHERE extraction_hour >= NOW() - INTERVAL '7 days'
    ORDER BY extraction_hour DESC
    """
    return pd.read_sql(query, engine)

try:
    df = load_data_quality_metrics()

    if not df.empty:
        avg_quality = df["data_quality_score"].mean()
        excellent_pct = (df["quality_category"] == "Excellent").mean() * 100
        total_records = df["total_records"].sum()

        col1, col2, col3 = st.columns(3)
        col1.metric("Average Quality Score", f"{avg_quality:.1f}%")
        col2.metric("Excellent Quality %", f"{excellent_pct:.1f}%")
        col3.metric("Total Records Analyzed", f"{total_records:,}")

        st.markdown("### 📈 Quality Trend Over Time")
        daily_quality = df.groupby(df["extraction_hour"].dt.date)["data_quality_score"].mean().reset_index()
        fig = px.line(daily_quality, x="extraction_hour", y="data_quality_score", title="Daily Data Quality Score")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### 📊 Quality by Cryptocurrency")
        symbol_quality = df.groupby("symbol")["data_quality_score"].mean().reset_index()
        fig2 = px.bar(symbol_quality, x="symbol", y="data_quality_score", title="Avg Quality per Symbol")
        st.plotly_chart(fig2, use_container_width=True)

    else:
        st.warning("No data quality metrics available for the last 7 days.")

except Exception as e:
    st.error(f"❌ Failed to load data quality metrics.\n\nError: {str(e)}")
