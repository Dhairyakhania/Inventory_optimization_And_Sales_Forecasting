from grafanalib.core import (
    Dashboard, Graph, Time, YAxis, YAxes, GridPos,
    Templating, Template, SqlTarget
)
from grafanalib._gen import DashboardEncoder
import json
import os

# -----------------------------
# Configuration
# -----------------------------
DATA_SOURCE = "PostgreSQL"  # Name of your Grafana Postgres data source
METRICS = ["forecast", "safety_stock", "reorder_quantity"]

# -----------------------------
# Template variables (for dynamic filtering)
# -----------------------------
templates = [
    Template(
        name="store_id",
        label="Store",
        query="SELECT DISTINCT store_id FROM warehouse.forecast_results ORDER BY store_id",
        multi=True,
        includeAll=True,
    ),
    Template(
        name="product_id",
        label="Product",
        query="SELECT DISTINCT product_id FROM warehouse.forecast_results ORDER BY product_id",
        multi=True,
        includeAll=True,
    ),
]

templating = Templating(list=templates)

# -----------------------------
# Generate panels
# -----------------------------
panels = []
for i, metric in enumerate(METRICS):
    panels.append(
        Graph(
            title=f"{metric.replace('_',' ').title()} Over Time",
            dataSource=DATA_SOURCE,
            targets=[
                SqlTarget(
                    rawSql=f"""
                        SELECT
                            date AS "time",
                            {metric} AS value
                        FROM warehouse.forecast_results
                        WHERE store_id = $store_id
                          AND product_id = $product_id
                        ORDER BY date
                    """,
                    refId=f"A{i}",
                )
            ],
            yAxes=YAxes(left=YAxis(format="short"), right=YAxis(format="short")),
            gridPos=GridPos(h=8, w=24, x=0, y=i * 8),
        )
    )

# -----------------------------
# Create dashboard
# -----------------------------
dashboard = Dashboard(
    title="Forecast Results Dashboard - All Stores",
    templating=templating,
    time=Time("now-90d", "now"),
    panels=panels,
).auto_panel_ids()

# -----------------------------
# Export JSON
# -----------------------------
# Make sure these directories exist
output_dir_local = os.path.abspath(os.path.join("..", "grafana", "dashboards"))
output_dir_provision = os.path.abspath(os.path.join("..", "grafana", "provisioning", "dashboards"))
os.makedirs(output_dir_local, exist_ok=True)
os.makedirs(output_dir_provision, exist_ok=True)

# Paths to save dashboard
output_path_local = os.path.join(output_dir_local, "forecast_dashboard.json")
output_path_provision = os.path.join(output_dir_provision, "forecast_dashboard.json")

# Write JSON files
with open(output_path_local, "w") as f:
    json.dump(dashboard, f, cls=DashboardEncoder, indent=2)

with open(output_path_provision, "w") as f:
    json.dump(dashboard, f, cls=DashboardEncoder, indent=2)

print(f"[INFO] Grafana dashboard JSON generated:\n- Local: {output_path_local}\n- Provisioning: {output_path_provision}")
