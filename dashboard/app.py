import logging
import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()


# ─────────────────────────────
# ⚙️ PAGE CONFIG (must be first Streamlit call!)
# ─────────────────────────────
st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded",
)


# Add health check endpoint
@st.cache_resource
def is_healthy():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


# Health check route for Docker
if st.experimental_get_query_params().get("healthz") is not None:
    if is_healthy():
        st.write("OK")
        st.stop()
    else:
        st.write("ERROR")
        st.stop()

# ─────────────────────────────
# 🪵 LOGGING SETUP
# ─────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─────────────────────────────
# 🔌 DATABASE CONNECTION
# ─────────────────────────────
@st.cache_resource
def get_engine():
    try:
        # Check for required environment variables
        required_env_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

        conn_str = (
            f"postgresql://{os.getenv('DB_USER')}:"
            f"{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:"
            f"{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )
        return create_engine(conn_str)
    except Exception as e:
        logger.exception("Database connection failed")
        st.error("❌ Could not connect to the database.")
        raise


engine = get_engine()

# ─────────────────────────────
# 🧊 SIDEBAR CONTROLS
# ─────────────────────────────
st.sidebar.header("Controls")
auto_refresh = st.sidebar.checkbox("Auto Refresh (every 30s)", value=False)
selected_cryptos = st.sidebar.multiselect(
    "Cryptos to Display",
    ["BTC", "ETH", "ADA", "DOT", "LINK"],
    default=["BTC", "ETH", "ADA"],
)

if auto_refresh:
    time.sleep(30)
    st.experimental_rerun()


# ─────────────────────────────
# 📦 DATA LOADER FUNCTIONS
# ─────────────────────────────
@st.cache_data(ttl=60)
def load_latest_prices():
    query = """
    SELECT symbol, crypto_name, current_price, market_cap,
           volume_24h, price_change_pct_24h, market_cap_rank,
           price_trend_category, market_cap_category, last_updated
    FROM public_marts.mart_crypto_latest_prices
    ORDER BY market_cap_rank
    """
    return pd.read_sql(query, engine)


@st.cache_data(ttl=300)
def load_hourly_performance(hours=48):
    query = """
    SELECT symbol, crypto_name, extraction_hour, avg_price_usd,
           hourly_change_pct, price_volatility, volatility_category,
           data_quality_score
    FROM public_marts.mart_crypto_hourly_performance
    WHERE extraction_hour >= NOW() - INTERVAL '%s hours'
    ORDER BY extraction_hour DESC
    """
    return pd.read_sql(query, engine, params=(hours,))


@st.cache_data(ttl=300)
def load_pipeline_monitoring():
    query = """
    SELECT run_date, stage, total_runs, successful_runs,
           success_rate_pct, avg_duration_seconds, latest_run_status,
           total_records_processed
    FROM public_marts.mart_pipeline_monitoring
    WHERE run_date >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY run_date DESC, stage
    """
    return pd.read_sql(query, engine)


@st.cache_data(ttl=60)
def load_system_health():
    latest_query = "SELECT MAX(extracted_at) as latest FROM crypto_prices_raw"
    stats_query = """
    SELECT COUNT(*) as total_records, COUNT(DISTINCT symbol) as unique_symbols,
           AVG(current_price) as avg_price
    FROM crypto_prices_raw
    WHERE extracted_at >= NOW() - INTERVAL '1 hour'
    """
    return pd.read_sql(latest_query, engine), pd.read_sql(stats_query, engine)


# ─────────────────────────────
# 🏥 SYSTEM HEALTH SECTION
# ─────────────────────────────
st.title("₿ Crypto Data Pipeline Dashboard")
st.markdown("---")
st.subheader("🏥 System Health")

try:
    latest_df, count_df = load_system_health()
    latest_time = pd.to_datetime(latest_df["latest"].iloc[0])

    if pd.isna(latest_time) or latest_time is None:
        st.warning("⚠️ No records found yet. Please run the pipeline to ingest data.")
        latest_time = datetime.utcnow()
        minutes_since = 9999
    else:
        minutes_since = (datetime.utcnow() - latest_time).total_seconds() / 60

    total_records = int(count_df.get("total_records", [0])[0])
    unique_symbols = int(count_df.get("unique_symbols", [0])[0])
    avg_price = float(count_df.get("avg_price", [0])[0] or 0)

    health_status = "🟢 Healthy" if minutes_since <= 120 else "🔴 Unhealthy"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("System Status", health_status, f"{int(minutes_since)} min ago")
    col2.metric("Records (Last Hour)", f"{total_records:,}")
    col3.metric("Active Symbols", unique_symbols)
    col4.metric("Avg Price", f"${avg_price:,.2f}")

except Exception as e:
    logger.exception("Failed to load system health")
    st.error("⚠️ Failed to load system health metrics. Ensure dbt models are built.")

st.markdown("---")

# ─────────────────────────────
# 💰 LATEST PRICES SECTION
# ─────────────────────────────
st.subheader("💰 Latest Crypto Prices")

try:
    prices_df = load_latest_prices()
    if selected_cryptos:
        prices_df = prices_df[prices_df["symbol"].isin(selected_cryptos)]

    col1, col2, col3 = st.columns(3)
    for i, row in prices_df.iterrows():
        col = [col1, col2, col3][i % 3]
        with col:
            trend = row["price_change_pct_24h"]
            icon = "🟢" if trend > 0 else "🔴" if trend < 0 else "⚪"
            st.metric(
                f"{icon} {row['symbol']} ({row['crypto_name']})",
                f"${row['current_price']:,.4f}",
                f"{trend:+.2f}%",
            )

    st.dataframe(
        prices_df[
            [
                "symbol",
                "crypto_name",
                "current_price",
                "price_change_pct_24h",
                "market_cap",
                "volume_24h",
                "market_cap_rank",
                "price_trend_category",
            ]
        ],
        use_container_width=True,
    )
except Exception as e:
    logger.exception("Failed to load price data")
    st.error("⚠️ Could not load latest crypto prices.")

st.markdown("---")

# ─────────────────────────────
# 📊 PERFORMANCE ANALYTICS
# ─────────────────────────────
st.subheader("📊 Performance Analytics")

try:
    hourly_df = load_hourly_performance()
    if selected_cryptos:
        hourly_df = hourly_df[hourly_df["symbol"].isin(selected_cryptos)]

    if not hourly_df.empty:
        st.plotly_chart(
            px.line(
                hourly_df,
                x="extraction_hour",
                y="avg_price_usd",
                color="symbol",
                title="Price Trend (Last 48 Hours)",
            ),
            use_container_width=True,
        )

        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                hourly_df.groupby("symbol")["price_volatility"].mean().reset_index(),
                x="symbol",
                y="price_volatility",
                title="Average Volatility",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.histogram(
                hourly_df,
                x="hourly_change_pct",
                color="symbol",
                title="Hourly Change Distribution",
                marginal="box",
            )
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    logger.exception("Performance analytics error")
    st.error("⚠️ Failed to load performance analytics.")

st.markdown("---")

# ─────────────────────────────
# 🔧 PIPELINE MONITORING
# ─────────────────────────────
st.subheader("🔧 Pipeline Monitoring")

try:
    monitor_df = load_pipeline_monitoring()

    if not monitor_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                monitor_df.groupby("stage")["success_rate_pct"].mean().reset_index(),
                x="stage",
                y="success_rate_pct",
                color="success_rate_pct",
                title="Avg Success Rate by Stage",
                color_continuous_scale="RdYlGn",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            volume_df = (
                monitor_df.groupby("run_date")["total_records_processed"]
                .sum()
                .reset_index()
            )
            fig = px.line(
                volume_df,
                x="run_date",
                y="total_records_processed",
                title="Daily Records Processed",
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Recent Pipeline Runs")
        st.dataframe(
            monitor_df.head(10)[
                [
                    "run_date",
                    "stage",
                    "total_runs",
                    "successful_runs",
                    "success_rate_pct",
                    "avg_duration_seconds",
                    "latest_run_status",
                ]
            ],
            use_container_width=True,
        )
    else:
        st.info("No recent pipeline runs found.")

except Exception as e:
    logger.exception("Pipeline monitoring load error")
    st.error("⚠️ Could not load pipeline monitoring data.")

# ─────────────────────────────
# 🧾 FOOTER
# ─────────────────────────────
st.markdown("---")
st.markdown(
    "🚀 **Crypto Data Pipeline Dashboard** — Built with ❤️ using Streamlit, Plotly, SQLAlchemy & PostgreSQL"
)

if st.sidebar.checkbox("Show Debug Info"):
    st.write(f"🕒 Current Time: {datetime.now().isoformat()}")
    st.write(f"🔎 Selected Cryptos: {selected_cryptos}")
