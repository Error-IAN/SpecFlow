# generator/template_registry.py
import os

# Get the project root (.../aedpa)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Define your Airflow Dags folder explicitly
# CHANGE THIS if your airflow home is different
AIRFLOW_DAGS_DIR = "/home/error-ian/airflow/dags"
TEMPLATE_REGISTRY = {
    "xlsx_to_duckdb_raw": {
        "template_path": "templates/ingest/xlsx_to_duckdb_raw.py.j2",
        "output_dir": "generated/ingest",
        "output_ext": ".py",
        "output_name": "{task_id}"
    },
    "orders_cleaning": {
        "template_path": "templates/transform/orders_cleaning.sql.j2",
        "output_dir": "generated/sql",
        "output_ext": ".sql",
        "output_name": "{task_id}"
    },
    "daily_sales_kpis": {
        "template_path": "templates/transform/daily_sales_kpis.sql.j2",
        "output_dir": "generated/sql",
        "output_ext": ".sql",
        "output_name": "{task_id}"
    },
    "airflow_dag": {
        "template_path": "templates/dag/airflow_dag.py.j2",
        "output_dir": AIRFLOW_DAGS_DIR,
        "output_ext": ".py",
        "output_name": "{dag_id}"
    }
}
