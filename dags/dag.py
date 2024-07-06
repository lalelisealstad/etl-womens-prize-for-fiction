from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# Import functions from scripts folder
from scripts.collect_data import get_wikidata
from scripts.collect_data import get_book_topics
from scripts.transform_data import transform_books
from scripts.transform_data import transform_topics

import sys
import os

# Add the parent directory of 'dags' to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
# from airflow.providers.google.cloud.operators.gcs import GCSToBigQueryOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def write_csv_locally(df):
    df.to_csv('../.test.csv', index=False)

default_args = {
    'start_date': days_ago(1),
    'description': "DAG to collect WPF data", 
    'project_id': 'wpf-pipline',
}

dag =  DAG('wpf_etl_pipeline', 
        default_args=default_args,
        schedule_interval='@yearly')
    
# Step 1: Extract data
extract_books_task = PythonOperator(
        task_id='extract_books',
        python_callable=get_wikidata(),
        dag=dag,
    )
# Task 2: Transform books data
def transform_books_wrapper(**kwargs):
    ti = kwargs['ti']
    books = ti.xcom_pull(task_ids='extract_books')
    transform_books(books)

transform_books_task = PythonOperator(
    task_id='transform_books',
    python_callable=transform_books_wrapper,
    provide_context=True,
    dag=dag,
)

# # Task 3: Extract book topics
# def extract_book_topics(**kwargs):
#     ti = kwargs['ti']
#     books = ti.xcom_pull(task_ids='extract_books')
#     return get_book_topics(books)

# extract_topics_task = PythonOperator(
#     task_id='extract_book_topics',
#     python_callable=extract_book_topics,
#     provide_context=True,
#     dag=dag,
# )

# # Task 4: Transform book topics
# def transform_topics_wrapper(**kwargs):
#     ti = kwargs['ti']
#     book_topics = ti.xcom_pull(task_ids='extract_book_topics')
#     transform_topics(book_topics)

# transform_topics_task = PythonOperator(
#     task_id='transform_topics',
#     python_callable=transform_topics_wrapper,
#     provide_context=True,
#     dag=dag,
# )


# Task 5: Save transformed books data to CSV using pandas
def save_books_to_csv(**kwargs):
    ti = kwargs['ti']
    transformed_books = ti.xcom_pull(task_ids='transform_books')
    df_books = pd.DataFrame(transformed_books, columns=['Book ID', 'Title', 'Author', 'Publication Year'])
    df_books.to_csv('/path/to/save/transformed_books.csv', index=False)

save_books_to_csv_task = PythonOperator(
    task_id='save_books_to_csv',
    python_callable=save_books_to_csv,
    provide_context=True,
    dag=dag,
)

# # Task 6: Save transformed topics data to CSV using pandas
# def save_topics_to_csv(**kwargs):
#     ti = kwargs['ti']
#     transformed_topics = ti.xcom_pull(task_ids='transform_topics')
#     df_topics = pd.DataFrame(transformed_topics, columns=['Book ID', 'Topic'])
#     df_topics.to_csv('/path/to/save/transformed_topics.csv', index=False)

# save_topics_to_csv_task = PythonOperator(
#     task_id='save_topics_to_csv',
#     python_callable=save_topics_to_csv,
#     provide_context=True,
#     dag=dag,
# )

# Define task dependencies
extract_books_task >> [transform_books_task] >> transform_books_task >> save_books_to_csv_task
# extract_books_task >> [transform_books_task, extract_topics_task]
# extract_topics_task >> transform_topics_task
# transform_books_task >> save_books_to_csv_task
# transform_topics_task >> save_topics_to_csv_task
    
    # # Step 3: Load data into GCS
    # load_to_gcs_task = GCSToBigQueryOperator(
    #     task_id='load_to_gcs_task',
    #     bucket='your-bucket-name',
    #     source_objects=['data/*.csv'],
    #     destination_project_dataset_table='your-project.your_dataset.your_table',
    #     write_disposition='WRITE_TRUNCATE',
    # )
    
    # # Step 4: Load data into BigQuery
    # load_to_bigquery_task = GCSToBigQueryOperator(
    #     task_id='load_to_bigquery_task',
    #     bucket='your-bucket-name',
    #     source_objects=['data/*.csv'],
    #     destination_project_dataset_table='your-project.your_dataset.your_table',
    #     write_disposition='WRITE_TRUNCATE',
    # )
    
