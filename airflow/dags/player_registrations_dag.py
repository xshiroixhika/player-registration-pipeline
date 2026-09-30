"""
Daily player registration pipeline.

extract (replica -> S3) -> bronze (Auto Loader) -> source freshness
  -> dbt build (silver + gold + tests) -> Tableau refresh

`dbt build` runs each model's tests right after building it and skips anything
downstream of a failure. So if silver or reconciliation tests fail, gold is not
rebuilt, Tableau isn't refreshed, and the dashboard keeps yesterday's verified numbers.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/repo/dbt")
DBT = f"cd {DBT_DIR} && dbt"


def extract_registrations():
    from extract.extract_registrations import run
    run()


def refresh_tableau_extract():
    import tableauserverclient as TSC

    auth = TSC.PersonalAccessTokenAuth(
        Variable.get("tableau_token_name"),
        Variable.get("tableau_token_secret"),
        site_id=Variable.get("tableau_site"),
    )
    server = TSC.Server(Variable.get("tableau_server_url"), use_server_version=True)
    with server.auth.sign_in(auth):
        datasource = server.datasources.get_by_id(Variable.get("tableau_datasource_id"))
        server.datasources.refresh(datasource)


def alert_on_failure(context):
    # Swap in Slack / PagerDuty / email. Keep the message actionable.
    task = context["task_instance"]
    print(f"ALERT: {task.dag_id}.{task.task_id} failed. Log: {task.log_url}")


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="player_registrations_daily",
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * *",          # 05:00 UTC, leaving room for the 08:00 UTC SLA
    catchup=False,
    max_active_runs=1,             # never let two runs write at the same time
    default_args=default_args,
    tags=["registrations", "gold"],
) as dag:

    extract = PythonOperator(
        task_id="extract_to_s3",
        python_callable=extract_registrations,
    )

    load_bronze = DatabricksRunNowOperator(
        task_id="load_bronze_autoloader",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.bronze_autoloader_job_id }}",
    )

    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} deps && {DBT} source freshness --target prod",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --target prod",
        sla=timedelta(hours=3),
    )

    refresh_tableau = PythonOperator(
        task_id="refresh_tableau_extract",
        python_callable=refresh_tableau_extract,
    )

    extract >> load_bronze >> source_freshness >> dbt_build >> refresh_tableau
