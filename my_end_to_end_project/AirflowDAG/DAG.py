# dags/end_to_end_load.py
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ─── Default arguments applied to all tasks ────────────────────────────────
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [abhijitghadge29@gmail.com],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Define the DAG ────────────────────────────────────────────────────────
with DAG(
    dag_id="end_to_end_snowflake_dbt_load",
    default_args=default_args,
    description="1) Incremental Python load from SQL Server → Snowflake  2) dbt run",
    schedule_interval="@daily",           # run once a day at midnight
    start_date=datetime(2025, 7, 12),     # change to your desired first run
    catchup=False,
    max_active_runs=1,
) as dag:

    # ─── 1) PythonOperator: run your incremental loader ────────────────────
    def run_incremental_loader():
        """
        Invokes your Python script that reads SQL Server and merges into Snowflake.
        Make sure the path below matches where you keep that script on your
        Airflow worker.
        """
        script_path = os.path.join(
            os.getenv("AIRFLOW_HOME", "/usr/local/airflow"),
            "dags/DI/incremental_load_SQL_TO_SF.py"
        )
        # You can also import and call a function directly if you package it.
        os.system(f"python {script_path}")

    incremental_task = PythonOperator(
        task_id="incremental_load_sql_to_sf",
        python_callable=run_incremental_loader,
        dag=dag,
    )

    # ─── 2) BashOperator: run dbt ───────────────────────────────────────────
    # Assumes dbt is installed in your Airflow environment and that
    # your AIRFLOW_HOME/dags/my_end_to_end_project folder contains the dbt project.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd $AIRFLOW_HOME/dags/my_end_to_end_project && "
            "dbt run --profiles-dir ~/.dbt --target dev"
        ),
        env={
            # if you need any env vars for your Snowflake creds, set them here
             "SOURCE_DATABASE": "SQL_SERVER_STAGE",
             "SOURCE_SCHEMA":   "dbo",
             "TARGET_DATABASE": "DEV_SNOWFLAKE_WAREHOUSE",
             "TARGET_SCHEMA":   "staging",
        },
        dag=dag,
    )

    # ─── Set task dependencies ──────────────────────────────────────────────
    incremental_task >> dbt_run
