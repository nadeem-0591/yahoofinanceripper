from flask import Flask, request
import mysql.connector
from selenium import webdriver
from datetime import datetime, timedelta

app = Flask(__name__)

# MySQL database configuration
config = {
    'user': 'root',
    'password': 'Neeha@123',
    'host': 'localhost',
    'database': 'telusko'
}

# Web scraping configuration
symbol = "AAPL"


@app.route('/historical-data', methods=['POST'])
def scrape_historical_data():
    # Retrieve start and end dates from JSON payload
    start_date = request.json.get('start_date')
    end_date = request.json.get('end_date')

    # Check if start and end dates are present
    if not start_date or not end_date:
        return 'Start and end dates are required!', 400

    # Convert start and end dates to datetime objects
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

    # Check if data already exists in database
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM Scraped_Data 
        WHERE symbol = %s AND start_date = %s AND end_date = %s
    """, (symbol, start_date, end_date))

    if len(cursor.fetchall()) > 0:
        cursor.close()
        conn.close()
        return 'Historical data already exists!', 400

    # Add one day to the end date to ensure all data from end date is included
    end_date_obj += timedelta(days=1)

    # Format start and end dates to be included in the URL
    start_date_url = str(int(start_date_obj.timestamp()))
    end_date_url = str(int(end_date_obj.timestamp()))
    url = f'https://finance.yahoo.com/quote/{symbol}/history?period1={start_date_url}&period2={end_date_url}&interval=1d&filter=history&frequency=1d'

    # Scrape data from Yahoo Finance website
    # Scrape data from Yahoo Finance website
    driver = webdriver.Chrome()
    driver.get(url)
    table = driver.find_element(by='xpath', value='//table[@data-test="historical-prices"]')
    headers = [th.text for th in table.find_elements(by="xpath",value='.//thead//th')]
    rows = [[td.text for td in tr.find_elements(by="xpath",value=".//td")] for tr in table.find_elements(by="xpath",value=".//tbody/tr")]
    driver.quit()

    # Create MySQL table with headers
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    create_table_query = f"CREATE TABLE IF NOT EXISTS {symbol}_Historical_Datawo ("
    for header in headers:
        header = header.replace('*', '_').replace(' ', '_')
        create_table_query += f"{header} VARCHAR(255), "
    create_table_query = create_table_query[:-2] + ");"

    cursor.execute(create_table_query)

    # Insert data into the table
    insert_query = f"INSERT INTO {symbol}_Historical_Datawo ("
    for header in headers:
        header = header.replace('*', '_').replace(' ', '_')
        insert_query += f"{header}, "
    insert_query = insert_query[:-2] + ") VALUES ("
    insert_query += ",".join(["%s" for _ in range(len(headers))]) + ");"

    for row in rows:
        cursor.execute(insert_query, row)

    conn.commit()

    cursor.close()
    conn.close()

    # Create a new table to store the start and end dates of scraped data
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    create_table_query = f"CREATE TABLE IF NOT EXISTS scraped_dates (symbol VARCHAR(255), start_date DATE, end_date DATE);"
    cursor.execute(create_table_query)

    # Check if data for the symbol and dates already exists in scraped_dates table
    select_query = f"SELECT * FROM scraped_dates WHERE symbol = '{symbol}' AND start_date = '{start_date}' AND end_date = '{end_date}';"
    cursor.execute(select_query)
    rows = cursor.fetchall()

    if len(rows) > 0:
        cursor.close()
        conn.close()
        return 'Historical data already exists!', 400

    # Insert the symbol and dates into scraped_dates table
    insert_query = f"INSERT INTO scraped_dates (symbol, start_date, end_date) VALUES ('{symbol}', '{start_date}', '{end_date}');"
    cursor.execute(insert_query)
    conn.commit()

    cursor.close()
    conn.close()

    return 'Historical data scraped and stored successfully!'

if __name__ == '__main__':
    app.run(debug=True)
