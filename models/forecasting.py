import os
import pandas as pd
import numpy as np
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.ensemble import GradientBoostingRegressor
from sqlalchemy import create_engine, MetaData, insert, text
from sqlalchemy.dialects.postgresql import insert

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
# Load all sales data
# -----------------------------
def load_sales_data():
    engine = get_postgres_engine()
    query = """
        SELECT date, store_id, product_id, quantity_sold
        FROM warehouse.sales
        ORDER BY date ASC
    """
    return pd.read_sql(query, engine)


# -----------------------------
# Forecasting functions
# -----------------------------
def prophet_forecast(df, horizon=30):
    ts = df.rename(columns={"date": "ds", "quantity_sold": "y"})
    if len(ts) < 2:
        return pd.DataFrame()
    model = Prophet()
    model.fit(ts)
    future = model.make_future_dataframe(periods=horizon)
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
        "date": pd.date_range(start=ts.index[-1]+pd.Timedelta(days=1), periods=horizon),
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
        "date": pd.date_range(start=ts.index[-1]+pd.Timedelta(days=1), periods=horizon),
        "forecast": forecast,
        "yhat_lower": forecast*0.9,
        "yhat_upper": forecast*1.1,
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
        "date": pd.date_range(start=ts["date"].max()+pd.Timedelta(days=1), periods=horizon),
        "forecast": forecast,
        "yhat_lower": forecast*0.9,
        "yhat_upper": forecast*1.1,
        "model": "GB"
    })
    return forecast_df


# -----------------------------
# Inventory metrics
# -----------------------------
def compute_inventory_metrics(df):
    df["forecast"] = df["forecast"].clip(lower=0)
    df["yhat_lower"] = df["yhat_lower"].clip(lower=0)
    df["yhat_upper"] = df["yhat_upper"].clip(lower=0)
    df["safety_stock"] = (df["forecast"].rolling(7, min_periods=1).std().fillna(0) * 1.65).clip(lower=0)
    df["reorder_quantity"] = (df["forecast"] + df["safety_stock"]).clip(lower=0)
    return df


# -----------------------------
# Upsert forecasts to Postgres
# -----------------------------
def upsert_forecast(df):
    engine = get_postgres_engine()
    metadata = MetaData(schema="public")
    metadata.reflect(bind=engine, only=["forecast_results"])
    table = metadata.tables["public.forecast_results"]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = insert(table).values(**row.to_dict())
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "store_id", "product_id", "model"],
                set_={
                    "forecast": stmt.excluded.forecast,
                    "yhat_lower": stmt.excluded.yhat_lower,
                    "yhat_upper": stmt.excluded.yhat_upper,
                    "safety_stock": stmt.excluded.safety_stock,
                    "reorder_quantity": stmt.excluded.reorder_quantity
                }
            )
            conn.execute(stmt)
    print(f"[INFO] Upserted {len(df)} rows into forecast_results")


# -----------------------------
# Run forecasts and store
# -----------------------------
def forecast_all_products(horizon=30):
    sales_df = load_sales_data()
    all_results = []

    for (store_id, product_id), group in sales_df.groupby(["store_id", "product_id"]):
        try:
            forecasts = []
            for model_func in [prophet_forecast, arima_forecast, ets_forecast, gb_forecast]:
                fcast = model_func(group, horizon=horizon)
                if not fcast.empty:
                    fcast = compute_inventory_metrics(fcast)
                    fcast["store_id"] = store_id
                    fcast["product_id"] = product_id
                    forecasts.append(fcast)

            if forecasts:
                # Average forecasts from all models
                combined = pd.concat(forecasts)
                combined_avg = combined.groupby("date").agg({
                    "forecast": "mean",
                    "yhat_lower": "mean",
                    "yhat_upper": "mean",
                    "safety_stock": "mean",
                    "reorder_quantity": "mean"
                }).reset_index()
                combined_avg["store_id"] = store_id
                combined_avg["product_id"] = product_id
                combined_avg["model"] = "AVERAGE"
                all_results.append(combined_avg)
        except Exception as e:
            print(f"[ERROR] Forecast failed for {store_id}-{product_id}: {e}")

    if all_results:
        result_df = pd.concat(all_results, ignore_index=True)
        upsert_forecast(result_df)
    else:
        print("[WARN] No forecasts generated")


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    forecast_all_products(horizon=30)
