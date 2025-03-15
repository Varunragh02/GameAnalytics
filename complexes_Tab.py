import requests
import json
import pandas as pd
import schedule
import time
from sqlalchemy import create_engine, text

# API URL and headers
url = "https://api.sportradar.com/tennis/trial/v3/en/complexes.json?api_key=3dUQ9pQLy122TLHBCb7eInTMSRRHBh56RqYciW8b"
headers = {"accept": "application/json"}

# Database connection
engine = create_engine("mysql+mysqlconnector://root:Selvamk1403#@localhost/gameanalytics")

# Function to fetch and store data
def fetch_and_store():
    try:
        # Sending GET request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for HTTP errors
        data = response.json()

        # Extracting data from JSON
        complexes = data.get('complexes', [])

        # Lists for data storage
        complex_data = []
        venues_data = []

        # Iterating through complexes
        for complex_item in complexes:
            complex_id = complex_item.get('id', None)
            complex_name = complex_item.get('name', None)

            # Append complex data
            complex_data.append([complex_id, complex_name])

            # Extract and iterate over venues (Fix applied here)
            venues = complex_item.get('venues', [])  # Ensure venues is a list
            for venue in venues:
                venue_id = venue.get('id', None)
                venue_name = venue.get('name', None)
                city_name = venue.get('city_name', None)
                country_name = venue.get('country_name', None)
                country_code = venue.get('country_code', None)
                timezone = venue.get('timezone', None)

                # Append venue data
                venues_data.append([venue_id, venue_name, city_name, country_name, country_code, timezone, complex_id])

        # Create DataFrames
        df_complex = pd.DataFrame(complex_data, columns=['complex_id', 'complex_name'])
        df_venues = pd.DataFrame(venues_data, columns=['venue_id', 'venue_name', 'city_name', 'country_name', 'country_code', 'timezone', 'complex_id'])

        # Remove duplicates
        df_complex.drop_duplicates(inplace=True)
        df_venues.drop_duplicates(inplace=True)

        # Debugging: Print DataFrames
        print("Complex Data:")
        print(df_complex.head())
        print("\nVenues Data:")
        print(df_venues.head())

        if df_complex.empty or df_venues.empty:
            print("No new data to insert into MySQL.")
            return

        with engine.begin() as conn:
            try:
                # Insert or Update Complex Data
                complex_sql = text("""
                    INSERT INTO complex_data (complex_id, complex_name)
                    VALUES (:complex_id, :complex_name)
                    ON DUPLICATE KEY UPDATE complex_name = VALUES(complex_name);
                """)
                complex_data_dicts = df_complex.to_dict(orient='records')
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
                venues_data_dicts = df_venues.to_dict(orient='records')
                conn.execute(venues_sql, venues_data_dicts)

                print("Data inserted successfully into MySQL!")

            except Exception as db_err:
                print("Database Insert Error:", db_err)

    except requests.exceptions.RequestException as api_err:
        print("API Request Error:", api_err)
    except Exception as e:
        print("An error occurred:", e)

# Schedule script to run every 6 hours
schedule.every(6).hours.do(fetch_and_store)

# Run script continuously
if __name__ == "__main__":
    fetch_and_store()  # Run once immediately
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute.
