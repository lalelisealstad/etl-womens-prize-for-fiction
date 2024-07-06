from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# # Import functions from scripts folder
# from scripts.collect_data import get_wikidata
# from scripts.collect_data import get_book_topics
# from scripts.transform_data import transform_books
# from scripts.transform_data import transform_topics

import sys
import os

# Add the parent directory of 'dags' to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from bs4 import BeautifulSoup
import requests 

def get_wikidata():
    """
    Function that uses webscrapping to get the tables of the List_of_Women's_Prize_for_Fiction_winners wikipedia page into one pandas dataframe
    
    Returns:
    pd.DataFrame
    
    """
    url = "https://en.wikipedia.org/wiki/List_of_Women's_Prize_for_Fiction_winners"
    
    # Send a GET request to the URL and retrieve the content
    response = requests.get(url)
    content = response.content

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(content, "html.parser")

    # Find all the tables on the page
    tables = soup.find_all("table", class_="wikitable")

    df = pd.DataFrame([])

    for table in tables: 
        # Create the first column Year. I do this in this way because the column has merged cells
        # Extract the table rows
        rows = []
        for row in table.find_all("tr"):
            rows.append(row)

        # Create an empty list to store the values of the first column
        first_column = []
        # Print the DataFrame
        # Iterate over the rows and extract the year value from the <a> tag
        for row in rows:
            cells = row.find_all(["th", "td"])
            if cells:
                # Check if the first cell has rowspan attribute
                if cells[0].has_attr("rowspan"):
                    # Get the year value from the <a> tag
                    year = cells[0].find("a").text.strip()
                    
                    # Get the rowspan value
                    rowspan = int(cells[0]["rowspan"])
                    
                    # Repeat the year value based on rowspan
                    first_column.extend([year] * rowspan)


        # Create empty lists to store the column values
        second_column = []
        third_column = []
        fourth_column = []

        # Extract the data from the second, third, and fourth columns
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                second_column.append(cells[0].text.strip())
                third_column.append(cells[1].text.strip())
                fourth_column.append(cells[2].text.strip())

        # Create a DataFrame from the column values
        data = {
            "Year" : first_column, 
            "Author": second_column,
            "Title": third_column,
            "Result": fourth_column
        }
        datadf = pd.DataFrame(data)
        df = pd.concat([datadf, df])
        
        df.to_csv('../.test.csv')

    return df

# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
# from airflow.providers.google.cloud.operators.gcs import GCSToBigQueryOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def write_csv_locally(df):
    df.to_csv('../.test.csv', index=False)

default_args = {
    'start_date': days_ago(1),
    'description': "DAG to collect WPF data", 
    'project_id': 'wpf-pipline',
    'depends_on_past' : False
}

dag =  DAG('wpf_etl_dag', 
        default_args=default_args, 
        schedule_interval='@yearly')
    
# Step 1: Extract data
extract_books_task = PythonOperator(
        task_id='extract_books',
        python_callable=get_wikidata,
        dag=dag,
    )
# # Task 2: Transform books data
# def transform_books_wrapper(**kwargs):
#     ti = kwargs['ti']
#     books = ti.xcom_pull(task_ids='extract_books')
#     transform_books(books)

# transform_books_task = PythonOperator(
#     task_id='transform_books',
#     python_callable=transform_books_wrapper,
#     provide_context=True,
#     dag=dag,
# )

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


# # Task 5: Save transformed books data to CSV using pandas
# def save_books_to_csv(**kwargs):
#     ti = kwargs['ti']
#     transformed_books = ti.xcom_pull(task_ids='extract_books_task')
#     df_books = pd.DataFrame(transformed_books, columns=['Book ID', 'Title', 'Author', 'Publication Year'])
#     df_books.to_csv('/path/to/save/transformed_books.csv', index=False)

# save_books_to_csv_task = PythonOperator(
#     task_id='save_books_to_csv',
#     python_callable=save_books_to_csv,
#     provide_context=True,
#     dag=dag,
# )

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
extract_books_task 
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
    
