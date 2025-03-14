import requests
import json
import pandas as pd
import mysql.connector
import schedule
import time
from sqlalchemy import create_engine, String
from sqlalchemy.sql import text

# API URL and headers
url = "https://api.sportradar.com/tennis/trial/v3/en/complexes.json?api_key=3dUQ9pQLy122TLHBCb7eInTMSRRHBh56RqYciW8b"
headers = {"accept": "application/json"}

# Database connection
engine = create_engine("mysql+mysqlconnector://root:Selvamk1403#@localhost/gameanalytics")

# Function to fetch and store data
def fetch_and_store():
    try:
        # Sending GET request and getting response text
        response = requests.get(url, headers=headers)
        print("API Response:")
        print(response.text)

        # Parsing JSON
        data = json.loads(response.text)
        print("JSON Data:")
        print(data)

        # Extracting data from JSON
        complexes = data.get('complexes', [])

        # Lists for data storage
        complex_data = []
        venues_data = []

        # Iterating through complexes
        for complex_item in complexes:
            complex_id = complex_item.get('id', None)
            complex_name = complex_item.get('name', None)

            # Extract venues from each complex
            venues = complex_item.get('venues', [])
            for venue in venues:
                venue_id = venue.get('id', None)
                venue_name = venue.get('name', None)
                city_name = venue.get('city_name', None)
                country_name = venue.get('country_name', None)
                country_code = venue.get('country_code', None)
                timezone = venue.get('timezone', None)

                # Append data
                complex_data.append([complex_id, complex_name])
                venues_data.append([venue_id, venue_name, city_name, country_name, country_code, timezone, complex_id])

        # Create DataFrames
        df_complex = pd.DataFrame(complex_data, columns=['complex_id', 'complex_name'])
        df_venues = pd.DataFrame(venues_data, columns=['venue_id', 'venue_name', 'city_name', 'country_name', 'country_code', 'timezone', 'complex_id'])

        # Remove duplicates
        df_complex.drop_duplicates(inplace=True)
        df_venues.drop_duplicates(inplace=True)

        # Debugging: Print DataFrames before inserting into MySQL
        print("Complex Data:")
        print(df_complex.head())
        print("\nVenues Data:")
        print(df_venues.head())

        with engine.begin() as conn:
            try:
                # Insert or Update Complex Data
                complex_sql = text("""
                    INSERT INTO complex_data (complex_id, complex_name)
                    VALUES (:complex_id, :complex_name)
                    ON DUPLICATE KEY UPDATE complex_name = VALUES(complex_name);
                """)
                complex_data_dicts = [{"complex_id": row[0], "complex_name": row[1]} for row in df_complex.values]
                conn.execute(complex_sql, complex_data_dicts)

                # Insert or Update Venues Data
                venues_sql = text("""
                    INSERT INTO venues_data (venue_id, venue_name, city_name, country_name, country_code, timezone, complex_id)
                    VALUES (:venue_id, :venue_name, :city_name, :country_name, :country_code, :timezone, :complex_id)
                    ON DUPLICATE KEY UPDATE 
                        venue_name = VALUES(venue_name),
                        city_name = VALUES(city_name),
                        country_name = VALUES(country_name),
                        country_code = VALUES(country_code),
                        timezone = VALUES(timezone),
                        complex_id = VALUES(complex_id);
                """)
                venues_data_dicts = [
                    {
                        "venue_id": row[0], "venue_name": row[1], "city_name": row[2],
                        "country_name": row[3], "country_code": row[4], "timezone": row[5], "complex_id": row[6]
                    } for row in df_venues.values
                ]
                conn.execute(venues_sql, venues_data_dicts)

                print("Data inserted successfully into MySQL with foreign key constraint!")

            except Exception as db_err:
                print("Database Insert Error:", db_err)

    except Exception as e:
        print("An error occurred:", e)

# Schedule script to run every 6 hours
schedule.every(6).hours.do(fetch_and_store)

# Run script continuously
if __name__ == "__main__":
    fetch_and_store()  # Run once immediately
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute