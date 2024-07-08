from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator 


from dag_etl import wpf_extract_transform_books, wpf_extract_transform_topics, wpf_load_books, wpf_load_topics


default_args = {
    'start_date': days_ago(1),
    'description': "DAG to collect WPF data", 
    'project_id': 'wpf-pipline',
    'depends_on_past' : False
}

dag =  DAG('wpf_etl_dag1', 
        default_args=default_args, 
        schedule_interval='@yearly')

run_et_books = PythonOperator(
        task_id='extract_transfrorm_books',
        python_callable=wpf_extract_transform_books,
        dag=dag,
)

run_et_topics = PythonOperator(
        task_id='extract_transfrorm_topics',
        python_callable=wpf_extract_transform_topics,
        dag=dag,
    )

run_load_books = PythonOperator(
        task_id='load_books',
        python_callable=wpf_load_books,
        dag=dag,
    )

run_load_topics = PythonOperator(
        task_id='load_topics',
        python_callable=wpf_load_topics,
        dag=dag,
    )


ready = DummyOperator(task_id='ready')

# run_et_books >> run_et_topics >> run_load_books >> run_load_topics >> ready 
run_et_books >> ready 