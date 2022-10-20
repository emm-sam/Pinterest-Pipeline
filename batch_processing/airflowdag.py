from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable

pinterest_dir = Variable.get("pinterest_dir")

default_args = {
    'owner': 'Emma',
    'depends_on_past': False,
    'email': ['emma.samouelle@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id='pinterest_dag',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['pinterest']
    
) as dag:
    batch_task = BashOperator(
        task_id="run_s3_spark_cassandra",
        bash_command="cd /Users/emmasamouelle/Desktop/Scratch/pinterest_project/pinterest_pipeline && python3 s3_spark.py",
    )