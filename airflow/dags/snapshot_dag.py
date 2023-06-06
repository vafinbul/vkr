from datetime import timedelta
import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

args = {'owner': 'airflow'}

dag_psql = DAG(
    dag_id="dag_snapshot",
    default_args=args,
    schedule_interval='* * * * *',
    dagrun_timeout=timedelta(minutes=60),
    description='Snapshot DAG',
    start_date=airflow.utils.dates.days_ago(1)
)

src_conn_id = 'pg_data'  # Идентификатор соединения с исходной базой данных PostgreSQL
trg_conn_id = 'pg_snapshot'  # Идентификатор соединения с целевой базой данных PostgreSQL


def insert_user_tables(**kwargs):
    src = PostgresHook(postgres_conn_id=src_conn_id)
    target = PostgresHook(postgres_conn_id=trg_conn_id)
    src_cursor = src.get_cursor()

    sql = """SELECT
    NOW()::timestamp AS timestamp,
    c.relname,
    COUNT(*) AS buffer_count
FROM
    pg_buffercache AS bc
JOIN
    pg_class AS c ON bc.relfilenode = c.relfilenode
    where c.oid >= 16384
GROUP BY
    c.relname;
    """

    src_cursor.execute(sql)
    rows = src_cursor.fetchall()
    target.insert_rows(table="buffercache", rows=rows, target_fields=['timestamp', 'relname', 'buffer_count'])
    target.commit()

insert_user_tables_task = PythonOperator(
    task_id="insert_user_tables",
    python_callable=insert_user_tables,
    dag=dag_psql
)

insert_user_tables_task

if __name__ == "__main__":
    dag_psql.cli()
