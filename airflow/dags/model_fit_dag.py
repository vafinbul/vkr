from datetime import timedelta
import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import pickle

args = {'owner': 'airflow'}

dag_psql = DAG(
    dag_id="dag_fit_model",
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    description='Model fit DAG',
    start_date=airflow.utils.dates.days_ago(1)
)

src_conn_id = 'pg_snapshot'  # Идентификатор соединения с целевой базой данных PostgreSQL


def fit_model(**kwargs):
    src = PostgresHook(postgres_conn_id=src_conn_id)
    src_cursor = src.get_cursor()

    fit_sql = "SELECT timestamp, relname, buffer_count FROM buffercache where (relname='pgbench_accounts' or relname='pgbench_branches' or relname='pgbench_tellers' or relname='pgbench_history') ORDER BY timestamp;"
    mae_sql = "SELECT mae FROM model order by timestamp desc limit 1;"
    src_cursor.execute(mae_sql)
    mae_best = src_cursor.fetchall()[0]
    df = pd.read_sql_query(fit_sql, src_conn_id)
    le = LabelEncoder()

    # Закодировать столбец relname
    df['relname_encoded'] = le.fit_transform(df['relname'])
    timestamp_numeric = df['timestamp'].apply(lambda x: x.timestamp()).values.reshape(-1, 1)
    print(df)
    # Выделение признаков и целевой переменной
    X = df[['relname_encoded']]
    X['timestamp'] = timestamp_numeric
    y = df['buffer_count']

    # Разделение данных на обучающую и тестовую выборки. Здесь 20% данных отводится под тестовую выборку.
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Инициализация модели случайного леса
    rf_model = RandomForestRegressor(n_estimators=61, random_state=42)

    # Обучение модели
    rf_model.fit(X_train, y_train)

    # Предсказания на тестовом наборе
    y_pred = rf_model.predict(X_test)

    # Вычисление MAE
    mae = mean_absolute_error(y_test, y_pred)
    if mae < mae_best:
        with open('rf_model.pkl', 'wb') as f:
            pickle.dump(rf_model, f)
        src_cursor.execute("INSERT INTO model(timestamp, mae) values (now(), %s)", (mae_best, ))



model_fit_task = PythonOperator(
    task_id="fit model",
    python_callable=fit_model,
    dag=dag_psql
)

model_fit_task

if __name__ == "__main__":
    dag_psql.cli()
