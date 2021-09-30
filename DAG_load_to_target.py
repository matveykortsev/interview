from datetime import datetime, timedelta
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor import ClickHouseSqlSensor
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(1, hour=6),
    "retries": 2,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


with DAG(
        dag_id="load_to_target",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        tags=['clickhouse'],
        concurrency=30
) as dag:


    check_stage = ClickHouseSqlSensor(
        task_id='check_stage',
        clickhouse_conn_id = 'clickhouse',
        database='default',
        sql="SELECT count(*) FROM stage",
        success=lambda cnt: cnt > 0
    )

    create_target_table = ClickHouseOperator(
        task_id='create_target_table',
        database='default',
        clickhouse_conn_id='clickhouse',
        sql="""
            CREATE TABLE IF NOT EXISTS target(
                    eventDate DateTime,
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
                    registration DateTime,
                    gender String,
                    artist String,
                    song String,
                    length Decimal32(5)
                ) ENGINE = MergeTree()
                ORDER BY (eventDate, userId)
                PARTITION BY (toStartOfMonth(eventDate))
            """
    )

    load_data_to_target = ClickHouseOperator(
        task_id='load_data_to_target',
        database='default',
        clickhouse_conn_id='clickhouse',
        sql= """
        INSERT INTO target
        SELECT 
                FROM_UNIXTIME(toInt64(ts / 1000)) as eventDate,
                userId,
                sessionId,
                page,
                auth,
                method,
                status,
                level,
                itemInSession,
                location,
                userAgent,
                lastName,
                firstName,
                FROM_UNIXTIME(toInt64(ts / 1000)) as registration,
                gender,
                artist,
                song,
                length
        FROM stage
        """
    )

    truncate_stage_table = ClickHouseOperator(
        task_id='truncate_stage_table',
        database='default',
        clickhouse_conn_id='clickhouse',
        sql= 'TRUNCATE TABLE IF EXISTS stage')


    check_stage >> create_target_table >> load_data_to_target >> truncate_stage_table