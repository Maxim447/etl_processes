import pandas as pd

OUTPUT = "/opt/airflow/dags/output"
INPUT = "/opt/airflow/input/IOT-temp.csv"

def transform_data():
    df = pd.read_csv(INPUT)

    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date

    hot_days = df.sort_values('temp', ascending=False).head(5)
    cold_days = df.sort_values('temp', ascending=True).head(5)

    df = df[df['out/in'] == 'In']

    p5 = df['temp'].quantile(0.05)
    p95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]

    df.to_csv(f"{OUTPUT}/clean_data.csv", index=False)
    hot_days.to_csv(f"{OUTPUT}/hot_days.csv", index=False)
    cold_days.to_csv(f"{OUTPUT}/cold_days.csv", index=False)