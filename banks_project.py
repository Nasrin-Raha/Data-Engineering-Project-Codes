
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

df = pd.DataFrame()

#Log Function

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    #timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


# All information
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'



#Extract Function

# Define the function to extract the required table
def extract(url):
    # Send an HTTP GET request to the URL
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        market_cap_heading = soup.find('span', {'id': 'By_market_capitalization'})
        
        if market_cap_heading:
            # Navigate to the table under the heading
            table = market_cap_heading.find_next('table')
            data = []

            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 3:
                    name = cols[0].text.strip()
                    market_cap = float(cols[2].text.strip()[:-1].replace(',', ''))
                    data.append([name, market_cap])

            df = pd.DataFrame(data, columns=['Bank Name', 'Market Cap (USD Billion)'])
            return df

    log_progress("Extraction failed: Table not found")
    return None

# URL of the Wikipedia page with the table
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

# Call the extract function and print the resulting DataFrame
extracted_df = extract(url)

if extracted_df is not None:
    print(extracted_df)
    log_progress("Extraction completed successfully")
else:
    print("Extraction failed. Please check the log for details.")

# Close the log entry with a final message
log_progress("End of extraction process")

# Close the log file after completion
log_progress("Log file closed.")




#Transformation Function

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Load exchange rate data from the CSV file
    exchange_rates = pd.read_csv(csv_path)

    # Ensure that the exchange rate columns are in the correct data types
    exchange_rates["GBP"] = exchange_rates["GBP"].astype(float)
    exchange_rates["EUR"] = exchange_rates["EUR"].astype(float)
    exchange_rates["INR"] = exchange_rates["INR"].astype(float)

    # Convert Market Cap from Billion USD to Million USD
    df["MC_USD_Billion"] = df["MC_USD_Billion"] * 1000

    # Convert Market Cap to GBP, EUR, and INR
    if "MC_USD_Billion" in df.columns:
        df["MC_GBP_Million"] = df["MC_USD_Billion"] * exchange_rates["GBP"]
        df["MC_EUR_Million"] = df["MC_USD_Billion"] * exchange_rates["EUR"]
        df["MC_INR_Million"] = df["MC_USD_Billion"] * exchange_rates["INR"]
    else:
        print("MC_USD_Billion column not found in the input DataFrame.")

    # Round the new columns to 2 decimal places
    df["MC_GBP_Million"] = df["MC_GBP_Million"].round(2)
    df["MC_EUR_Million"] = df["MC_EUR_Million"].round(2)
    df["MC_INR_Million"] = df["MC_INR_Million"].round(2)

    return df

# Call the transform function with both the data frame and the CSV path
if df is not None:
    df = transform(df, csv_path)


#Load to CSV Function

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)


#Load to DB Function

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


#Run Query Function


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''



#Log Process 


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')
"""
query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)
"""

# Create a SQLite3 connection
sql_connection = sqlite3.connect('Banks.db')
# Query 1: Print the contents of the entire table
query_statement_1 = "SELECT * FROM Largest_banks"
run_queries(query_statement_1, sql_connection)

# Query 2: Print the average market capitalization of all the banks in Billion USD
query_statement_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(query_statement_2, sql_connection)

# Query 3: Print only the names of the top 5 banks
query_statement_3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_queries(query_statement_3, sql_connection)


log_progress('Process Complete.')

sql_connection.close()