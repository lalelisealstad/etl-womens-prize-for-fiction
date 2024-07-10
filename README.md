# womens-prize-for-fiction-data
Simple ETL pipeline using Apache Airflow to get a dataset of Women's prize for ficiton nominees and winners and the genres of the books. 

Steps: 
1. Web scraping to extract a list of Women's prize for fiction nominees and winners from wikipedia. 
2. Collect meta data from Open Library API. 
3. Data cleaning
4. Stores data as parquet files 
5. loads data in SQLlite database 


Install Python env : 
```
$ python3 -m venv .venv
$ source .venv/bin/activate 
$ pip install -r requirements.txt
```

Set up Airflow: 
```
$ source .venv/bin/activate
$ export AIRFLOW_HOME=~/airflow
$ airflow db init
$ airflow scheduler
$ airflow webserver
```
