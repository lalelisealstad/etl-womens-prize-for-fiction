##  E T L ##

import logging
log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

# Extract and transform 
import requests
import pandas as pd
from bs4 import BeautifulSoup


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
            
    return df


## BOOK TOPICS FROM OPEN LIBRARY API
def get_book_topics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collects topics of books using Open Library API.
    The output is a DataFrame with 'Book' and 'Topic' columns.
    
    Params: df (pandas DataFrame) - DataFrame containing 'Title' and 'Author' columns.
    
    Returns: topics_df (pandas DataFrame) - DataFrame with book titles and corresponding topics.
    
    """
    import requests
    import pandas as pd

    def get_book_topics(title, author):
        base_url = 'http://openlibrary.org/search.json'
        params = {
            'title': title,
            'author': author,
            'limit': 1
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raise an exception for non-successful status codes
            data = response.json()

            if 'docs' in data and len(data['docs']) > 0:
                book = data['docs'][0]
                topics = book.get('subject', [])
                return topics
            else:
                return []
        except Exception as e:
            print(f"Error occurred for title: {title}, author: {author}")
            return []

    books = []
    topics = []

    for _, row in df.iterrows():
        title = row['Title']
        author = row['Author']
        book_topics = get_book_topics(title, author)
        for topic in book_topics:
            books.append(title)
            topics.append(topic)

    topics_df = pd.DataFrame({'Book': books, 'Topic': topics})
    return topics_df


import re
import pandas as pd

# Some topics are combined. Such as Ficiton, general, or with , or --. I make a function to split these into seperate rows 
import re 
def split_rows(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    new_rows = []
    
    for idx, row in df.iterrows():
        # Check if the value contains a comma or a slash
        if ',' in row[column_name] or '--' in row[column_name] or ' /'in row[column_name]:
            # Split the value into separate words
            words = re.split(r'[,/]|--', row[column_name])
            # Create new rows for each word
            for word in words:
                new_row = row.copy()
                new_row[column_name] = word
                new_rows.append(new_row)
        else:
            new_rows.append(row)
    
    # Create a new DataFrame with the new rows
    new_df = pd.DataFrame(new_rows)
    
    return new_df

# Some titles are returned with wikipedia reference or added characthers, example, title[2]. I adjust the titles to remove the square brackets and the content within. 
# Function to remove square brackets and their contents using regex
def remove_square_brackets(text: str) -> str:
    return re.sub(r'\[.*?\]', '', text)


def remove_tags(df: pd.DataFrame, column_name: str, pattern: str) -> pd.DataFrame:
        return df[~df[column_name].str.contains(pattern, case=False, regex=True)]


def transform_topics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the given DataFrame.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame to transform.
    
    Returns:
    pd.DataFrame: Transformed DataFrame.
    """
    
    # Regex pattern to match "NYT: followed by words and ends with a datetime yyyy-mm-dd". This is a redundant tag so I remove it
    pattern = r'^nyt:[\w-]+=202\d-\d{2}-\d{2}$'

    df = remove_tags(df, 'Topic', pattern)

    # Split rows with multiple topics to seperate rows 
    df = split_rows(df, 'Topic')

    df['Topic'] = df['Topic'].str.lower()
    df = df.drop_duplicates(subset =  ['Book', 'Topic'])
    
    return df


def transform_books(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the given DataFrame.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame to transform.
    
    Returns:
    pd.DataFrame: Transformed DataFrame.
    """

    df.Year = df.Year.astype(int)

    df['Title'] = df['Title'].apply(remove_square_brackets)
    
    return df


def wpf_extract_transform_books(): 
    try: 
    # extract
        books = get_wikidata()
        # book_topics = get_book_topics(books)
        
        # transform 
        books = transform_books(books)
        # book_topics = transform_topics(book_topics)
        
        # load intermediary 
        books.to_parquet(f"books.parquet")
    
        log.info("Book extraction and transform complete")
    except Exception as e:
        log.error("Error message {e}")
    


def wpf_extract_transform_topics(): 
    try: 
        # extract
        books = pd.read_parquet(f"books.parquet")
        book_topics = get_book_topics(books)
        
        # transform 
        book_topics = transform_topics(book_topics)
        
        # load intermediary 
        book_topics.to_parquet(f"book_topics.parquet")
        
        log.info("Book topics extraction and transform complete")
    except Exception as e:
        log.error("Error message {e}", exc_info=True)  



### LOAD to SQLlite database 

import sqlite3
def wpf_load_books(): 
    try: 
        books = pd.read_parquet(f"books.parquet")

        conn = sqlite3.connect('wpf_books.db')

        books.to_sql('books', conn, if_exists='replace', index=False) 

        conn.close()
        log.info("Book load complete")
    except Exception as e:
        log.error("Error message {e}")  
        
    
def wpf_load_topics(): 
    try: 
        book_topics = pd.read_parquet(f"book_topics.parquet")

        conn = sqlite3.connect('wpf_books.db')
        
        book_topics.to_sql('book_topics', conn, if_exists='replace', index=False) 

        conn.close()
    
        log.info("Book topics load complete")
    except Exception as e:
        log.error("Error message {e}")  