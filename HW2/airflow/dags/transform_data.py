from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transform_data():
    df = pd.read_csv('/opt/airflow/data/temp.csv')

    #1
    coldest = df[df['out/in']=='Out'].sort_values('temp').head(5)
    hotest = df[df['out/in']=='Out'].sort_values('temp').tail(5)
    extrem_days = pd.concat([coldest,hotest])
    extrem_days.to_csv('/opt/airflow/data/extrem_days.csv', index=False)

    #2
    in_temp = df[df['out/in']=='In']
    in_temp.to_csv('/opt/airflow/data/in_temp.csv', index=False)

    #3
    df.noted_date = pd.to_datetime(df.noted_date, format = '%d-%m-%Y %H:%M').dt.date
    df.to_csv('/opt/airflow/data/date_format.csv', index=False)

    #4
    temp_5_95 = df[(df["temp"] >= df["temp"].quantile(0.05)) &
                   (df["temp"] <= df["temp"].quantile(0.95))]
    temp_5_95.to_csv('/opt/airflow/data/temp_5_95.csv', index=False)

with DAG(
    dag_id="transform_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "homework"],
) as dag:
    PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )