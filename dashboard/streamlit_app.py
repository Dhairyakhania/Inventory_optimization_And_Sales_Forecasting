# streamlit_app.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import altair as alt
import plotly.express as px
from datetime import datetime

# ---------------------------
# Page config
# ---------------------------
st.set_page_config(
    page_title="EDA + Forecast + Inventory Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Updated Altair theme for >=5.5.0
alt.themes.enable("dark")

# ---------------------------
# DB connection
# ---------------------------
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "warehouse")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

# ---------------------------
# Helpers
# ---------------------------
@st.cache_data(show_spinner=False, ttl=300)
def fetch_df(sql: str, params: dict = None) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params or {})
    return df

def csv_list(values):
    """Convert selection list to tuple for SQL IN clause"""
    if not values:
        return None
    return tuple(values)

def time_range_to_clause(start_ts, end_ts, col="date"):
    """Returns SQL fragment and params for time range"""
    return f" AND {col} >= :start_ts AND {col} <= :end_ts ", {"start_ts": start_ts, "end_ts": end_ts}

def filter_clause_for(ids, column):
    """Returns SQL fragment and params for IN clause, handles empty tuple"""
    if not ids:
        return "", {}
    param_name = f"vals_{column}"
    return f" AND {column} IN :{param_name} ", {param_name: ids}

# ---------------------------
# Sidebar filters
# ---------------------------
st.sidebar.header("Filters")

stores_df = fetch_df('SELECT DISTINCT "store_id" FROM "warehouse"."forecast_results" ORDER BY "store_id"')
products_df = fetch_df('SELECT DISTINCT "product_id" FROM "warehouse"."forecast_results" ORDER BY "product_id"')

all_stores = stores_df["store_id"].astype(str).tolist()
all_products = products_df["product_id"].astype(str).tolist()

sel_stores = st.sidebar.multiselect("Store", options=all_stores, default=all_stores)
sel_products = st.sidebar.multiselect("Product", options=all_products, default=all_products)

# Time range
default_start = pd.Timestamp.today() - pd.DateOffset(years=1)
default_end = pd.Timestamp.today()
start_date = st.sidebar.date_input("Start date", value=default_start.date())
end_date = st.sidebar.date_input("End date", value=default_end.date())

# Metrics config
FORECAST_METRICS = ["forecast", "safety_stock", "reorder_quantity"]

# Convert selections
store_tuple = csv_list(sel_stores)
product_tuple = csv_list(sel_products)
start_ts = pd.Timestamp(start_date)
end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)

# ---------------------------
# Header
# ---------------------------
st.title("Full EDA + Forecast + Inventory Optimization Dashboard")

# ---------------------------
# Forecast/Safety/Reorder over time
# ---------------------------
st.subheader("Forecast, Safety Stock, Reorder Quantity Over Time")
cols = st.columns(3)
for idx, metric in enumerate(FORECAST_METRICS):
    time_sql = f"""
        SELECT
            "date"::timestamp AS time,
            ("store_id"::text || ' - ' || "product_id"::text) AS series,
            AVG({metric}) AS value
        FROM "warehouse"."forecast_results"
        WHERE 1=1
    """
    params = {}
    # Add time filter
    time_clause, time_params = time_range_to_clause(start_ts, end_ts, col='"date"')
    time_sql += time_clause
    params.update(time_params)
    # Add store/product filters
    f1, p1 = filter_clause_for(store_tuple, "store_id")
    f2, p2 = filter_clause_for(product_tuple, "product_id")
    time_sql += f1 + f2 + " GROUP BY 1,2 ORDER BY 1"
    params.update(p1)
    params.update(p2)

    df = fetch_df(time_sql, params)
    if not df.empty:
        chart = alt.Chart(df).mark_line().encode(
            x=alt.X("time:T", title="Time"),
            y=alt.Y("value:Q", title=metric.replace("_", " ").title()),
            color=alt.Color("series:N", legend=alt.Legend(title="Store - Product")),
            tooltip=["time:T", "series:N", "value:Q"]
        ).properties(height=300).interactive()
        with cols[idx]:
            st.altair_chart(chart, use_container_width=True)
    else:
        with cols[idx]:
            st.info(f"No {metric.replace('_', ' ').title()} data in range.")

# ---------------------------
# Actual vs Forecast Revenue
# ---------------------------
st.subheader("Actual vs Forecast Revenue")

# Actual
sql_actual = """
    SELECT
        "date"::timestamp AS time,
        ("store_id"::text || ' - ' || "product_id"::text) AS series,
        SUM(revenue) AS value
    FROM "warehouse"."sales"
    WHERE 1=1
"""
params_actual = {}
cl, pr = time_range_to_clause(start_ts, end_ts, col='"date"')
sql_actual += cl
params_actual.update(pr)
f1, p1 = filter_clause_for(store_tuple, "store_id")
f2, p2 = filter_clause_for(product_tuple, "product_id")
sql_actual += f1 + f2 + " GROUP BY 1,2 ORDER BY 1"
params_actual.update(p1)
params_actual.update(p2)
df_actual = fetch_df(sql_actual, params_actual)

# Forecast
sql_forecast_rev = """
    SELECT
        "date"::timestamp AS time,
        ("store_id"::text || ' - ' || "product_id"::text) AS series,
        AVG(forecast) AS value
    FROM "warehouse"."forecast_results"
    WHERE 1=1
"""
params_forecast = {}
cl2, pr2 = time_range_to_clause(start_ts, end_ts, col='"date"')
sql_forecast_rev += cl2
params_forecast.update(pr2)
f1f, p1f = filter_clause_for(store_tuple, "store_id")
f2f, p2f = filter_clause_for(product_tuple, "product_id")
sql_forecast_rev += f1f + f2f + " GROUP BY 1,2 ORDER BY 1"
params_forecast.update(p1f)
params_forecast.update(p2f)
df_forecast = fetch_df(sql_forecast_rev, params_forecast)

left, right = st.columns(2)
with left:
    st.caption("Actual Revenue")
    if not df_actual.empty:
        ch_a = alt.Chart(df_actual).mark_line().encode(
            x="time:T", y=alt.Y("value:Q", title="Revenue"),
            color="series:N", tooltip=["time:T", "series:N", "value:Q"]
        ).properties(height=320).interactive()
        st.altair_chart(ch_a, use_container_width=True)
    else:
        st.info("No actual revenue data in range.")

with right:
    st.caption("Forecast Revenue")
    if not df_forecast.empty:
        ch_f = alt.Chart(df_forecast).mark_line(strokeDash=[4, 2]).encode(
            x="time:T", y=alt.Y("value:Q", title="Forecast"),
            color="series:N", tooltip=["time:T", "series:N", "value:Q"]
        ).properties(height=320).interactive()
        st.altair_chart(ch_f, use_container_width=True)
    else:
        st.info("No forecast data in range.")

# ---------------------------
# Monthly Revenue by Store
# ---------------------------
st.subheader("Monthly Revenue by Store")
sql_monthly = """
    SELECT
        date_trunc('month', "date"::timestamp) AS time,
        "store_id"::text AS series,
        SUM(revenue) AS value
    FROM "warehouse"."sales"
    WHERE 1=1
"""
params_m = {}
clm, prm = time_range_to_clause(start_ts, end_ts, col='"date"')
sql_monthly += clm
params_m.update(prm)
fms, pms = filter_clause_for(store_tuple, "store_id")
sql_monthly += fms + " GROUP BY 1,2 ORDER BY 1"
params_m.update(pms)
df_monthly = fetch_df(sql_monthly, params_m)

if not df_monthly.empty:
    ch_m = alt.Chart(df_monthly).mark_line().encode(
        x="time:T", y=alt.Y("value:Q", title="Revenue"),
        color="series:N", tooltip=["time:T", "series:N", "value:Q"]
    ).properties(height=320).interactive()
    st.altair_chart(ch_m, use_container_width=True)
else:
    st.info("No monthly revenue data in range.")

# ---------------------------
# Top Products by Revenue
# ---------------------------
st.subheader("Top Products by Revenue")
sql_top_products = """
    SELECT
        "product_id"::text AS product_id,
        SUM(revenue) AS revenue
    FROM "warehouse"."sales"
    WHERE 1=1
"""
params_tp = {}
fp, pp = filter_clause_for(product_tuple, "product_id")
sql_top_products += fp + " GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
params_tp.update(pp)
df_top = fetch_df(sql_top_products, params_tp)
st.dataframe(df_top, use_container_width=True)

# ---------------------------
# Revenue Share by Product
# ---------------------------
st.subheader("Revenue Share by Product")
sql_share = """
    SELECT
        "product_id"::text AS product_id,
        SUM(revenue) AS revenue
    FROM "warehouse"."sales"
    WHERE 1=1
"""
params_sh = {}
fs, ps = filter_clause_for(product_tuple, "product_id")
sql_share += fs + " GROUP BY 1"
params_sh.update(ps)
df_share = fetch_df(sql_share, params_sh)

if not df_share.empty:
    fig = px.pie(df_share, names="product_id", values="revenue", hole=0.35)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No revenue data for selected products.")

# ---------------------------
# Profit vs Revenue per Product
# ---------------------------
st.subheader("Profit vs Revenue per Product")
sql_profit = """
    SELECT
        "product_id"::text AS product_id,
        SUM(revenue) AS revenue,
        SUM(profit) AS profit
    FROM "warehouse"."sales"
    WHERE 1=1
"""
params_pr = {}
fpv, ppv = filter_clause_for(product_tuple, "product_id")
sql_profit += fpv + " GROUP BY 1"
params_pr.update(ppv)
df_profit = fetch_df(sql_profit, params_pr)
st.dataframe(df_profit, use_container_width=True)

# Footer
st.caption("Built with Streamlit, Altair, and Plotly.")
