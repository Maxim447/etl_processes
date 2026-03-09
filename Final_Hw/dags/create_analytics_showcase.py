from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

POSTGRES_CONN = "host=etl_postgres port=5432 dbname=etl_db user=airflow password=airflow"
SQL_DIR = Path('/opt/airflow/sql')

def get_pg_conn():
    return psycopg2.connect(POSTGRES_CONN)


def read_sql(filename: str) -> str:
    sql_path = SQL_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL-файл не найден: {sql_path}")
    return sql_path.read_text(encoding='utf-8')


def execute_sql_file(filename: str, **context):
    sql = read_sql(filename)
    pg = get_pg_conn()
    try:
        cursor = pg.cursor()
        cursor.execute(sql)
        pg.commit()

        table_name = filename.replace('.sql', '')
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        print(f"[{filename}] выполнен успешно. Строк в таблице: {count}")

        cursor.close()
    except Exception as e:
        pg.rollback()
        raise
    finally:
        pg.close()


def show_showcase_preview(**context):
    sql_raw = read_sql('showcase_preview.sql')
    pg = get_pg_conn()

    try:
        cursor = pg.cursor()

        queries = [
            q.strip()
            for q in sql_raw.split(';')
            if q.strip() and not q.strip().startswith('--')
        ]

        headers = [
            "Витрина 1: Топ активных пользователей",
            "Витрина 2: Эффективность поддержки",
        ]

        for idx, query in enumerate(queries):
            print("\n" + "=" * 60)
            if idx < len(headers):
                print(headers[idx])
            print("=" * 60)

            cursor.execute(query)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            header_line = " | ".join(f"{col:<18}" for col in col_names)
            print(header_line)
            print("-" * len(header_line))

            for row in rows:
                print(" | ".join(f"{str(val):<18}" for val in row))

        cursor.close()
    except Exception as e:
        raise
    finally:
        pg.close()


with DAG(
        dag_id='create_analytics_showcase',
        default_args=default_args,
        description='Построение аналитических витрин через внешние SQL-файлы',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['etl', 'analytics', 'showcase'],
) as dag:
    build_user_activity = PythonOperator(
        task_id='build_user_activity_showcase',
        python_callable=execute_sql_file,
        op_kwargs={'filename': 'showcase_user_activity.sql'},
    )

    build_support_efficiency = PythonOperator(
        task_id='build_support_efficiency_showcase',
        python_callable=execute_sql_file,
        op_kwargs={'filename': 'showcase_support_efficiency.sql'},
    )

    preview = PythonOperator(
        task_id='show_showcase_preview',
        python_callable=show_showcase_preview,
    )

    [build_user_activity, build_support_efficiency] >> preview