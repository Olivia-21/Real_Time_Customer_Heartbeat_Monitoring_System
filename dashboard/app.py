"""
app.py – Streamlit Real-Time Heartbeat Dashboard
=================================================
Live dashboard that auto-refreshes every 5 seconds, showing:
  • KPI cards  – total readings, anomaly rate, unique customers.
  • Line chart – last 5-minute rolling window of heart-rate per customer.
  • Anomaly table – most recent anomalies.
  • Pipeline log chart – ingest throughput over time.
"""

import os
import time

import pandas as pd
import psycopg2
import streamlit as st

# ──────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="💓 Heartbeat Monitor",
    page_icon="💓",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ──────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────
DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB", "heartbeat_db"),
    "user":     os.getenv("POSTGRES_USER", "heartbeat_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "heartbeat_pass"),
}


@st.cache_resource(show_spinner=False)
def get_conn():
    """Cached persistent connection (reused across reruns)."""
    return psycopg2.connect(**DB_PARAMS, connect_timeout=10)


def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        # On broken connection, clear cache and reconnect
        get_conn.clear()
        conn2 = get_conn()
        return pd.read_sql_query(sql, conn2, params=params)


# ──────────────────────────────────────────────
# Custom CSS
# ──────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .kpi-card {
        background: linear-gradient(135deg, #1e1e2e, #2a2a3e);
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
        border: 1px solid #3a3a5e;
    }
    .kpi-value { font-size: 2.4rem; font-weight: 700; color: #a6e3a1; margin: 0; }
    .kpi-label { font-size: 0.85rem; color: #cdd6f4; margin: 0; text-transform: uppercase; letter-spacing: 0.08em; }
    .anomaly-badge { color: #f38ba8; font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────
st.markdown("# 💓 Real-Time Customer Heartbeat Monitor")
st.markdown("Auto-refreshes every **5 seconds** · Data sourced from PostgreSQL")

refresh_placeholder = st.empty()

# ──────────────────────────────────────────────
# Main render function
# ──────────────────────────────────────────────
def render_dashboard():
    # ── KPIs ──────────────────────────────────
    kpi_df = query(
        """
        SELECT
            COUNT(*)                                                  AS total_readings,
            COUNT(DISTINCT customer_id)                               AS unique_customers,
            ROUND(100.0 * SUM(is_anomaly::int) / NULLIF(COUNT(*),0), 2) AS anomaly_rate_pct,
            COALESCE(MAX(recorded_at), NOW())                         AS last_seen
        FROM heart_rate_readings;
        """
    )

    row = kpi_df.iloc[0] if not kpi_df.empty else {}

    c1, c2, c3, c4 = st.columns(4)
    for col, label, value, unit in [
        (c1, "Total Readings",    row.get("total_readings", 0),    ""),
        (c2, "Unique Customers",  row.get("unique_customers", 0),   ""),
        (c3, "Anomaly Rate",      row.get("anomaly_rate_pct", 0.0), "%"),
        (c4, "Last Reading",      str(row.get("last_seen", "—"))[:19], ""),
    ]:
        col.markdown(
            f'<div class="kpi-card"><p class="kpi-value">{value}{unit}</p>'
            f'<p class="kpi-label">{label}</p></div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ── Rolling line chart (last 5 min, top 6 customers by count) ──
    st.subheader("📈 Heart Rate – Last 5 Minutes")

    top_customers_df = query(
        """
        SELECT customer_id
        FROM heart_rate_readings
        WHERE recorded_at >= NOW() - INTERVAL '5 minutes'
        GROUP BY customer_id
        ORDER BY COUNT(*) DESC
        LIMIT 6;
        """
    )

    if top_customers_df.empty:
        st.info("Waiting for data… start the producer container.")
    else:
        top_ids = tuple(top_customers_df["customer_id"].tolist())
        ts_df = query(
            f"""
            SELECT customer_id,
                   date_trunc('second', recorded_at) AS second,
                   ROUND(AVG(heart_rate)) AS avg_bpm
            FROM heart_rate_readings
            WHERE recorded_at >= NOW() - INTERVAL '5 minutes'
              AND customer_id IN ({', '.join(['%s'] * len(top_ids))})
            GROUP BY customer_id, second
            ORDER BY second;
            """,
            params=top_ids,
        )
        if not ts_df.empty:
            pivot = ts_df.pivot(index="second", columns="customer_id", values="avg_bpm")
            st.line_chart(pivot)

    st.markdown("---")

    col_left, col_right = st.columns([1, 1])

    # ── Latest anomalies ──
    with col_left:
        st.subheader("🚨 Recent Anomalies")
        anomaly_df = query(
            """
            SELECT customer_id,
                   to_char(recorded_at AT TIME ZONE 'UTC', 'HH24:MI:SS') AS time_utc,
                   heart_rate AS bpm,
                   anomaly_reason
            FROM heart_rate_readings
            WHERE is_anomaly = TRUE
            ORDER BY recorded_at DESC
            LIMIT 20;
            """
        )
        if anomaly_df.empty:
            st.success("No anomalies detected yet.")
        else:
            st.dataframe(
                anomaly_df.style.applymap(
                    lambda _: "color: #f38ba8; font-weight: 600",
                    subset=["bpm"],
                ),
                use_container_width=True,
                hide_index=True,
            )

    # ── Throughput chart ──
    with col_right:
        st.subheader("📊 Ingest Throughput (Pipeline Log)")
        log_df = query(
            """
            SELECT date_trunc('minute', logged_at) AS minute,
                   SUM(messages_valid)   AS valid,
                   SUM(messages_anomaly) AS anomaly,
                   SUM(messages_invalid) AS invalid
            FROM pipeline_log
            WHERE logged_at >= NOW() - INTERVAL '1 hour'
            GROUP BY minute
            ORDER BY minute;
            """
        )
        if log_df.empty:
            st.info("Pipeline log will appear after the first metrics flush (~60 s).")
        else:
            log_df = log_df.set_index("minute")
            st.bar_chart(log_df)

    # ── Per-customer latest BPM table ──
    st.markdown("---")
    st.subheader("👤 Per-Customer Latest Heart Rate")
    latest_df = query(
        """
        SELECT DISTINCT ON (customer_id)
               customer_id,
               heart_rate AS bpm,
               CASE WHEN is_anomaly THEN anomaly_reason ELSE 'Normal' END AS status,
               to_char(recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS recorded_at_utc
        FROM heart_rate_readings
        ORDER BY customer_id, recorded_at DESC;
        """
    )
    if not latest_df.empty:
        st.dataframe(latest_df, use_container_width=True, hide_index=True)


# ──────────────────────────────────────────────
# Auto-refresh loop
# ──────────────────────────────────────────────
REFRESH_INTERVAL = 5  # seconds

render_dashboard()

refresh_placeholder.caption(
    f"⏱ Next refresh in {REFRESH_INTERVAL}s  |  Last updated: "
    + pd.Timestamp.now(tz="UTC").strftime("%H:%M:%S UTC")
)

time.sleep(REFRESH_INTERVAL)
st.rerun()
