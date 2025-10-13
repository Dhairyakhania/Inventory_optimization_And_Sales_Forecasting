from grafanalib.core import (
    Dashboard, Graph, Table, PieChart, Time, YAxis, YAxes, GridPos,
    Templating, Template, SqlTarget
)
from grafanalib._gen import DashboardEncoder
import json
import os

DATA_SOURCE = "ForecastPostgres"
FORECAST_METRICS = ["forecast", "safety_stock", "reorder_quantity"]

# -----------------------------
# Template variables
# -----------------------------
templates = [
    Template(
        name="store_id",
        label="Store",
        query="SELECT DISTINCT store_id FROM warehouse.forecast_results ORDER BY store_id",
        multi=True,
        includeAll=True,
        allValue=""  # leave empty for all
    ),
    Template(
        name="product_id",
        label="Product",
        query="SELECT DISTINCT product_id FROM warehouse.forecast_results ORDER BY product_id",
        multi=True,
        includeAll=True,
        allValue=""
    ),
]

templating = Templating(list=templates)

# -----------------------------
# Panels
# -----------------------------
panels = []
y_pos = 0

# --- Forecast, Safety Stock, Reorder Quantity Over Time ---
for metric in FORECAST_METRICS:
    panels.append(
        Graph(
            title=f"{metric.replace('_',' ').title()} Over Time",
            dataSource=DATA_SOURCE,
            targets=[
                SqlTarget(
                    rawSql=f"""
                        SELECT
                            $__time(date::timestamp) AS time,
                            store_id || ' - ' || product_id AS metric,
                            AVG({metric}) AS value
                        FROM warehouse.forecast_results
                        WHERE
                            $__timeFilter(date::timestamp)
                            AND (COALESCE(NULLIF($store_id:csv, ''), store_id) = store_id)
                            AND (COALESCE(NULLIF($product_id:csv, ''), product_id) = product_id)
                        GROUP BY 1,2
                        ORDER BY 1
                    """,
                    refId=f"{metric}_time"
                )
            ],
            yAxes=YAxes(left=YAxis(format="short"), right=YAxis(format="short")),
            gridPos=GridPos(h=8, w=24, x=0, y=y_pos)
        )
    )
    y_pos += 8

# --- Actual vs Forecast Revenue ---
panels.append(
    Graph(
        title="Actual vs Forecast Revenue",
        dataSource=DATA_SOURCE,
        targets=[
            SqlTarget(
                rawSql="""
                    SELECT
                        $__time(date::timestamp) AS time,
                        store_id || ' - ' || product_id AS metric,
                        SUM(revenue) AS value
                    FROM warehouse.sales
                    WHERE
                        $__timeFilter(date::timestamp)
                        AND (COALESCE(NULLIF($store_id:csv, ''), store_id) = store_id)
                        AND (COALESCE(NULLIF($product_id:csv, ''), product_id) = product_id)
                    GROUP BY 1,2
                    ORDER BY 1
                """,
                refId="A_actual"
            ),
            SqlTarget(
                rawSql="""
                    SELECT
                        $__time(date::timestamp) AS time,
                        store_id || ' - ' || product_id AS metric,
                        AVG(forecast) AS value
                    FROM warehouse.forecast_results
                    WHERE
                        $__timeFilter(date::timestamp)
                        AND (COALESCE(NULLIF($store_id:csv, ''), store_id) = store_id)
                        AND (COALESCE(NULLIF($product_id:csv, ''), product_id) = product_id)
                    GROUP BY 1,2
                    ORDER BY 1
                """,
                refId="A_forecast"
            )
        ],
        yAxes=YAxes(left=YAxis(format="currency"), right=YAxis(format="currency")),
        gridPos=GridPos(h=8, w=24, x=0, y=y_pos)
    )
)
y_pos += 8

# --- Monthly Revenue by Store ---
panels.append(
    Graph(
        title="Monthly Revenue by Store",
        dataSource=DATA_SOURCE,
        targets=[
            SqlTarget(
                rawSql="""
                    SELECT
                        $__time_trunc(date::timestamp,'month') AS time,
                        store_id AS metric,
                        SUM(revenue) AS value
                    FROM warehouse.sales
                    WHERE
                        $__timeFilter(date::timestamp)
                        AND (COALESCE(NULLIF($store_id:csv, ''), store_id) = store_id)
                    GROUP BY 1,2
                    ORDER BY 1
                """,
                refId="E1"
            )
        ],
        yAxes=YAxes(left=YAxis(format="currency"), right=YAxis(format="currency")),
        gridPos=GridPos(h=8, w=24, x=0, y=y_pos)
    )
)
y_pos += 8

# --- Top Products by Revenue ---
panels.append(
    Table(
        title="Top Products by Revenue",
        dataSource=DATA_SOURCE,
        targets=[
            SqlTarget(
                rawSql="""
                    SELECT
                        product_id AS metric,
                        SUM(revenue) AS value
                    FROM warehouse.sales
                    WHERE
                        (COALESCE(NULLIF($product_id:csv, ''), product_id) = product_id)
                    GROUP BY 1
                    ORDER BY 2 DESC
                    LIMIT 10
                """,
                refId="E2"
            )
        ],
        gridPos=GridPos(h=6, w=24, x=0, y=y_pos)
    )
)
y_pos += 6

# --- Revenue Share by Product ---
panels.append(
    PieChart(
        title="Revenue Share by Product",
        dataSource=DATA_SOURCE,
        targets=[
            SqlTarget(
                rawSql="""
                    SELECT
                        product_id AS metric,
                        SUM(revenue) AS value
                    FROM warehouse.sales
                    WHERE
                        (COALESCE(NULLIF($product_id:csv, ''), product_id) = product_id)
                    GROUP BY 1
                """,
                refId="E3"
            )
        ],
        gridPos=GridPos(h=6, w=24, x=0, y=y_pos)
    )
)
y_pos += 6

# --- Profit vs Revenue per Product ---
panels.append(
    Table(
        title="Profit vs Revenue per Product",
        dataSource=DATA_SOURCE,
        targets=[
            SqlTarget(
                rawSql="""
                    SELECT
                        product_id AS metric,
                        SUM(revenue) AS revenue,
                        SUM(profit) AS profit
                    FROM warehouse.sales
                    WHERE
                        (COALESCE(NULLIF($product_id:csv, ''), product_id) = product_id)
                    GROUP BY 1
                """,
                refId="E4"
            )
        ],
        gridPos=GridPos(h=6, w=24, x=0, y=y_pos)
    )
)
y_pos += 6

# -----------------------------
# Dashboard
# -----------------------------
dashboard = Dashboard(
    title="Full EDA + Forecast + Inventory Optimization Dashboard",
    templating=templating,
    time=Time("now-1y","now"),
    panels=panels
).auto_panel_ids()

# -----------------------------
# Export JSON
# -----------------------------
output_dir_local = os.path.abspath(os.path.join("..","grafana","dashboards"))
output_dir_provision = os.path.abspath(os.path.join("..","grafana","provisioning","dashboards"))
os.makedirs(output_dir_local, exist_ok=True)
os.makedirs(output_dir_provision, exist_ok=True)

output_path_local = os.path.join(output_dir_local,"full_dashboard.json")
output_path_provision = os.path.join(output_dir_provision,"full_dashboard.json")

for path in [output_path_local, output_path_provision]:
    with open(path,"w") as f:
        json.dump(dashboard,f,cls=DashboardEncoder,indent=2)

print(f"[INFO] Grafana dashboard JSON generated:\n- Local: {output_path_local}\n- Provisioning: {output_path_provision}")
