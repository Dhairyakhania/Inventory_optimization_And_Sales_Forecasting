import pandas as pd
from sqlalchemy import create_engine

# Connect to Postgres
engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/warehouse")

# Query forecasts
df = pd.read_sql("""
    SELECT *
    FROM public.forecast_results
    ORDER BY date ASC
    LIMIT 20
""", engine)

print(df)
