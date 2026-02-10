import pandas as pd
import os

OUTPUT = "/opt/airflow/dags/output"
TARGET = "/opt/airflow/dags/target"
INPUT = "/opt/airflow/input/IOT-temp.csv"

CLEAN_DATA = f"{OUTPUT}/clean_data.csv"
HISTORY_TARGET = f"{TARGET}/temperature_history.csv"
INCREMENT_TARGET = f"{TARGET}/temperature_increment.csv"

def transform_data():
    df = pd.read_csv(INPUT)

    df['noted_date'] = pd.to_datetime(
        df['noted_date'],
        format='%d-%m-%Y %H:%M'
    ).dt.date

    hot_days = df.sort_values('temp', ascending=False).head(5)
    cold_days = df.sort_values('temp', ascending=True).head(5)

    df = df[df['out/in'] == 'In']

    p5 = df['temp'].quantile(0.05)
    p95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]

    os.makedirs(OUTPUT, exist_ok=True)

    df.to_csv(CLEAN_DATA, index=False)
    hot_days.to_csv(f"{OUTPUT}/hot_days.csv", index=False)
    cold_days.to_csv(f"{OUTPUT}/cold_days.csv", index=False)

def load_full_history():
    df = pd.read_csv(CLEAN_DATA)
    os.makedirs(TARGET, exist_ok=True)
    df.to_csv(HISTORY_TARGET, index=False)

def load_increment(**context):
    df = pd.read_csv(CLEAN_DATA)
    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf and "load_date" in dag_run.conf:
        load_date = pd.to_datetime(dag_run.conf["load_date"]).date()
    else:
        load_date = pd.Timestamp.now().date() - pd.Timedelta(days=1)

    df['noted_date'] = pd.to_datetime(df['noted_date']).dt.date
    increment_df = df[df['noted_date'] >= load_date]

    os.makedirs(TARGET, exist_ok=True)
    if os.path.exists(INCREMENT_TARGET):
        existing_df = pd.read_csv(INCREMENT_TARGET)
        result_df = pd.concat([existing_df, increment_df]).drop_duplicates()
    else:
        result_df = increment_df

    result_df.to_csv(INCREMENT_TARGET, index=False)