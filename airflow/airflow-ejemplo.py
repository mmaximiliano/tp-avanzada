import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id='test',
  schedule=None,
  start_date=datetime.datetime(2024, 4, 1),
  catchup=False,
) as dag:
  sleep = BashOperator(
    task_id='sleep',
    bash_command='sleep 3',
)