import os
import pandas as pd
import numpy as np
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.ensemble import GradientBoostingRegressor
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
from tqdm import tqdm

# -----------------------------
# Database connection
# -----------------------------
def get_postgres_engine():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "warehouse")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# -----------------------------
# Load sales data
# -----------------------------
def load_sales_data():
    engine = get_postgres_engine()
    query = """
        SELECT date, store_id, product_id, quantity_sold
        FROM warehouse.sales
        ORDER BY date ASC
    """
    df = pd.read_sql(query, engine)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df.dropna(subset=['date'])

# -----------------------------
# Forecasting functions
# -----------------------------
def prophet_forecast(df, horizon=30):
    ts = df.rename(columns={"date": "ds", "quantity_sold": "y"})
    if len(ts) < 2:
        return pd.DataFrame()
    model = Prophet()
    model.fit(ts)
    future = model.make_future_dataframe(periods=horizon, freq='D')
    future = future[future['ds'] >= pd.Timestamp.today().normalize()]
    forecast = model.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    forecast.rename(columns={"ds": "date", "yhat": "forecast"}, inplace=True)
    forecast["model"] = "Prophet"
    return forecast

def arima_forecast(df, horizon=30, order=(1, 1, 1)):
    ts = df.set_index("date")["quantity_sold"]
    if len(ts) < 5:
        return pd.DataFrame()
    model = ARIMA(ts, order=order)
    model_fit = model.fit()
    pred = model_fit.get_forecast(steps=horizon)
    forecast_df = pd.DataFrame({
        "date": pd.date_range(start=pd.Timestamp.today().normalize(), periods=horizon),
        "forecast": pred.predicted_mean,
        "yhat_lower": pred.conf_int().iloc[:, 0],
        "yhat_upper": pred.conf_int().iloc[:, 1],
        "model": "ARIMA"
    })
    return forecast_df

def ets_forecast(df, horizon=30):
    ts = df.set_index("date")["quantity_sold"]
    if len(ts) < 5:
        return pd.DataFrame()
    model = ExponentialSmoothing(ts, trend="add", seasonal=None)
    model_fit = model.fit()
    forecast = model_fit.forecast(horizon)
    forecast_df = pd.DataFrame({
        "date": pd.date_range(start=pd.Timestamp.today().normalize(), periods=horizon),
        "forecast": forecast,
        "yhat_lower": forecast * 0.9,
        "yhat_upper": forecast * 1.1,
        "model": "ETS"
    })
    return forecast_df

def gb_forecast(df, horizon=30):
    ts = df.set_index("date")["quantity_sold"].reset_index()
    if len(ts) < 5:
        return pd.DataFrame()
    ts["day"] = (ts["date"] - ts["date"].min()).dt.days
    X = ts[["day"]].values
    y = ts["quantity_sold"].values
    model = GradientBoostingRegressor()
    model.fit(X, y)
    future_days = np.arange(X[-1, 0]+1, X[-1, 0]+1+horizon).reshape(-1,1)
    forecast = model.predict(future_days)
    forecast_df = pd.DataFrame({
        "date": pd.date_range(start=pd.Timestamp.today().normalize(), periods=horizon),
        "forecast": forecast,
        "yhat_lower": forecast * 0.9,
        "yhat_upper": forecast * 1.1,
        "model": "GB"
    })
    return forecast_df

# -----------------------------
# Ensemble & inventory metrics
# -----------------------------
MODEL_WEIGHTS = {
    "Prophet": 0.4,
    "ARIMA": 0.3,
    "ETS": 0.2,
    "GB": 0.1
}

def weighted_ensemble(forecasts_list):
    if not forecasts_list:
        return pd.DataFrame()
    combined = pd.concat(forecasts_list, axis=0)
    combined["weight"] = combined["model"].map(MODEL_WEIGHTS).fillna(0)
    ensemble_df = combined.groupby("date").apply(
        lambda x: pd.Series({
            "forecast": (x["forecast"] * x["weight"]).sum() / x["weight"].sum(),
            "yhat_lower": (x["yhat_lower"] * x["weight"]).sum() / x["weight"].sum(),
            "yhat_upper": (x["yhat_upper"] * x["weight"]).sum() / x["weight"].sum()
        })
    ).reset_index()
    return ensemble_df

def compute_inventory_metrics(df):
    df["forecast"] = df["forecast"].clip(lower=0)
    df["yhat_lower"] = df["yhat_lower"].clip(lower=0)
    df["yhat_upper"] = df["yhat_upper"].clip(lower=0)
    df["safety_stock"] = (df["forecast"].rolling(7, min_periods=1).std().fillna(0) * 1.65).clip(lower=0)
    df["reorder_quantity"] = (df["forecast"] + df["safety_stock"]).clip(lower=0)
    return df

# -----------------------------
# Dynamic horizon calculation for all stores
# -----------------------------
def get_forecast_horizon(group, default=30):
    # Use 2x average days between sales as horizon
    sales_dates = group['date'].sort_values()
    if len(sales_dates) > 1:
        avg_diff = sales_dates.diff().dt.days.mean()
        return max(int(avg_diff*2), 7)  # minimum 7 days
    return default

# -----------------------------
# Upsert forecasts
# -----------------------------
def upsert_forecast(df):
    if df.empty:
        print("[WARN] No forecast data to store.")
        return

    numeric_cols = ["forecast", "yhat_lower", "yhat_upper", "safety_stock", "reorder_quantity"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    engine = get_postgres_engine()
    table_name = "forecast_results"
    schema_name = "warehouse"

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                date DATE,
                store_id VARCHAR(50),
                product_id VARCHAR(50),
                forecast NUMERIC(12,4),
                yhat_lower NUMERIC(12,4),
                yhat_upper NUMERIC(12,4),
                safety_stock NUMERIC(12,4),
                reorder_quantity NUMERIC(12,4),
                PRIMARY KEY (date, store_id, product_id)
            );
        """))

        records = df[["date","store_id","product_id","forecast","yhat_lower",
                      "yhat_upper","safety_stock","reorder_quantity"]].to_dict(orient="records")
        values = [(
            r["date"], r["store_id"], r["product_id"], r["forecast"],
            r["yhat_lower"], r["yhat_upper"], r["safety_stock"], r["reorder_quantity"]
        ) for r in records]

        insert_sql = f"""
            INSERT INTO {schema_name}.{table_name} (
                date, store_id, product_id, forecast, yhat_lower, yhat_upper,
                safety_stock, reorder_quantity
            )
            VALUES %s
            ON CONFLICT (date, store_id, product_id)
            DO UPDATE SET
                forecast = EXCLUDED.forecast,
                yhat_lower = EXCLUDED.yhat_lower,
                yhat_upper = EXCLUDED.yhat_upper,
                safety_stock = EXCLUDED.safety_stock,
                reorder_quantity = EXCLUDED.reorder_quantity;
        """
        conn.connection.cursor().execute("BEGIN;")
        execute_values(conn.connection.cursor(), insert_sql, values)
        conn.connection.cursor().execute("COMMIT;")

    print(f"[INFO] Upserted {len(df)} rows into {schema_name}.{table_name}")

# -----------------------------
# Main forecasting loop
# -----------------------------
def forecast_all_products(min_rows=5):
    sales_df = load_sales_data()
    grouped = sales_df.groupby(["store_id", "product_id"])
    all_results = []

    for (store_id, product_id), group in tqdm(grouped, desc="Forecasting all store-product groups"):
        group = group.sort_values('date').reset_index(drop=True)
        if len(group) < min_rows or group['quantity_sold'].sum() == 0:
            continue

        horizon = get_forecast_horizon(group)

        forecasts = []
        for model_func in [prophet_forecast, arima_forecast, ets_forecast, gb_forecast]:
            try:
                fcast = model_func(group, horizon=horizon)
                if fcast.empty:
                    continue
                forecasts.append(fcast)
            except Exception as e:
                print(f"[ERROR] Forecast failed for {store_id}-{product_id} ({model_func.__name__}): {e}")

        if forecasts:
            ensemble = weighted_ensemble(forecasts)
            ensemble["store_id"] = store_id
            ensemble["product_id"] = product_id
            ensemble = compute_inventory_metrics(ensemble)
            all_results.append(ensemble)

    if all_results:
        result_df = pd.concat(all_results, ignore_index=True)
        upsert_forecast(result_df)
    else:
        print("[WARN] No forecasts generated. Check your sales data.")

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    forecast_all_products()
