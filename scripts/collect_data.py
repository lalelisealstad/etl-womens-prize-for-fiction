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