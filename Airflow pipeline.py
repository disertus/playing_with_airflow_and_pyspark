from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd


def csv_to_json():
    df = pd.read_csv('data_engineering_test_file.csv')
    for i, r in df.iterrows():
        print(r['name'])
        df.to_json('from_airflow.json', orient='records')


default_args = {
    'owner': 'Roman',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'TestingAirflow',
        default_args=default_args,
        description='A simple Airflow pipeline for testing',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=['example']
) as dag:
    print_starting = BashOperator(task_id='starting',
                                  bash_command='echo "I am reading the CSV file now..."')
    csv_conversion = PythonOperator(task_id='convert_csv_to_json',
                                    python_callable=csv_to_json)

print_starting.set_downstream(csv_conversion)
