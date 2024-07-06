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