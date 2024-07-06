from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from dag_etl import run_wpf_etl


default_args = {
    'start_date': days_ago(1),
    'description': "DAG to collect WPF data", 
    'project_id': 'wpf-pipline',
    'depends_on_past' : False
}

dag =  DAG('new_dag', 
        default_args=default_args, 
        schedule_interval='@yearly')


    
# Step 1: Extract data
run_etl = PythonOperator(
        task_id='whole_wpf_etl',
        python_callable=run_wpf_etl,
        dag=dag,
    )

run_etl