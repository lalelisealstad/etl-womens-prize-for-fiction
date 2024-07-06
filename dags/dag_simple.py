from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas as pd


def get_wikidata():
    """
    Function that uses webscrapping to get the tables of the List_of_Women's_Prize_for_Fiction_winners wikipedia page into one pandas dataframe
    
    Returns:
    pd.DataFrame
    
    """
    print('hello!')
   

default_args = {
    'start_date': days_ago(1),
    'description': "DAG to collect WPF data", 
    'project_id': 'wpf-pipline',
    'depends_on_past' : False
}

dag =  DAG('wpf_etl_dag_copy', 
        default_args=default_args, 
        schedule_interval='@yearly')


    
# Step 1: Extract data
extract_books_task = PythonOperator(
        task_id='extract_books',
        python_callable=get_wikidata,
        dag=dag,
    )

extract_books_task 
