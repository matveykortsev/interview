from datetime import datetime, timedelta
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago

SSH_HOOK = SSHHook(ssh_conn_id='clickhouse_ssh')

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 2,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


with DAG(
        dag_id="load_to_stage",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        tags=['clickhouse'],
        concurrency=30
) as dag:

    
    load_data = SSHOperator(
        task_id="load_tables",
        command= "clickhouse-client --query 'INSERT INTO stage FORMAT JSONEachRow' < /var/lib/clickhouse/event-data.json",
        ssh_hook = SSH_HOOK,
        dag = dag)


    create_table = ClickHouseOperator(
        task_id='create_table',
        database='default',
        clickhouse_conn_id='clickhouse',
        sql="""
                CREATE TABLE IF NOT EXISTS stage(
                    ts UInt64,
                    userId String,
                    sessionId UInt8,
                    page String,
                    auth String,
                    method String,
                    status UInt8,
                    level String,
                    itemInSession UInt8,
                    location String,
                    userAgent String,
                    lastName String,
                    firstName String,
                    registration UInt64,
                    gender String,
                    artist String,
                    song String,
                    length Decimal32(5)
                ) ENGINE = Log
            """
    )

    create_table >> load_data