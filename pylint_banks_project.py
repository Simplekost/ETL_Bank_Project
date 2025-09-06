"""
ETL Script to extract bank market cap data from Wikipedia,
convert currencies using a CSV file, and store results in CSV and SQLite database.
"""
import sqlite3
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Constants
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_RATE_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
)
TABLE_ATTRIBS = ['Name', 'MC_USD_Billion']
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
CSV_PATH = 'Largest_banks_data.csv'
LOG_FILE = 'code_log.txt'

SQL_CONNECTION = sqlite3.connect(DB_NAME)


def log_progress(message):
    """Log ETL steps with timestamp to a text file."""
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now().strftime(timestamp_format)
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(f"{now} : {message}\n")


def extract(source_url, attributes):
    """Extract bank market cap data from Wikipedia."""
    html_content = requests.get(source_url).text
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('tbody')

    extracted_data = pd.DataFrame(columns=attributes)
    rows = tables[0].find_all("tr")

    for row_element in rows:
        cols = row_element.find_all('td')
        if not cols or cols[1].find('a') is None:
            continue

        bank_data = {
            attributes[0]: cols[1].text.strip(),
            attributes[1]: float(cols[2].text.strip())
        }

        temp_df = pd.DataFrame(bank_data, index=[0])
        extracted_data = pd.concat([extracted_data, temp_df], ignore_index=True)

    return extracted_data


def get_table_html(source_url):
    """Return prettified HTML of the first 'wikitable' for backup/reference."""
    html_page = requests.get(source_url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'})

    return table.prettify() if table else "Table not found"


def transform(bank_data_frame, exchange_url):
    """Add GBP, EUR, and INR equivalents to the extracted data."""
    exchange_df = pd.read_csv(exchange_url)
    exchange_columns = exchange_df.columns
    exchange_dict = exchange_df.set_index(exchange_columns[0]).to_dict()[exchange_columns[1]]

    bank_data_frame['MC_GBP_Billion'] = (
        bank_data_frame['MC_USD_Billion'] * exchange_dict.get('GBP')
    ).round(2)
    bank_data_frame['MC_EUR_Billion'] = (
        bank_data_frame['MC_USD_Billion'] * exchange_dict.get('EUR')
    ).round(2)
    bank_data_frame['MC_INR_Billion'] = (
        bank_data_frame['MC_USD_Billion'] * exchange_dict.get('INR')
    ).round(2)

    return bank_data_frame


def load_to_csv(dataframe, file_path):
    """Save transformed data to a CSV file."""
    dataframe.to_csv(file_path, index=False)


def load_to_db(dataframe, connection, table):
    """Load data into SQLite database table."""
    dataframe.to_sql(table, connection, if_exists='replace', index=False)


def run_query(statement, connection):
    """Execute SQL query and return DataFrame."""
    result = pd.read_sql(statement, connection)
    print(statement)
    print(result)
    return result


# === Main ETL Process ===

log_progress('Preliminaries complete. Initiating ETL process.')

extracted_data_frame = extract(URL, TABLE_ATTRIBS)
print(extracted_data_frame)

# Save HTML backup of the table
HTML_TABLE = get_table_html(URL)
with open("table_html.txt", "w", encoding="utf-8") as html_file:
    html_file.write(HTML_TABLE)

log_progress('Data extraction complete. Initiating Transformation process.')

transformed_data_frame = transform(extracted_data_frame, EXCHANGE_RATE_URL)
print(transformed_data_frame)

log_progress('Data transformation complete. Initiating Loading process.')

# Optional: check a specific value
print(transformed_data_frame.loc[4, 'MC_EUR_Billion'])

# Save to CSV and DB
load_to_csv(transformed_data_frame, CSV_PATH)
log_progress('Data saved to CSV file.')

load_to_db(transformed_data_frame, SQL_CONNECTION, TABLE_NAME)
log_progress('Data loaded into SQLite database.')

# Run sample queries
run_query("SELECT * FROM Largest_banks", SQL_CONNECTION)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", SQL_CONNECTION)

for result_row in run_query(
    "SELECT Name FROM Largest_banks LIMIT 5", SQL_CONNECTION
).itertuples(index=False):
    print(result_row)

log_progress('Process complete.')
SQL_CONNECTION.close()
log_progress('Server connection closed.')
