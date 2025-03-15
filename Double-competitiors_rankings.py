import requests
import json
import pandas as pd
import mysql.connector
import schedule
import time
from sqlalchemy import create_engine, String
from sqlalchemy.sql import text

# API URL & Headers
url = "https://api.sportradar.com/tennis/trial/v3/en/double_competitors_rankings.json?api_key=3dUQ9pQLy122TLHBCb7eInTMSRRHBh56RqYciW8b"
headers = {"accept": "application/json"}

# Database connection
engine = create_engine("mysql+mysqlconnector://root:Selvamk1403#@localhost/gameanalytics")

# Function to fetch and store data
def fetch_and_store():
    try:
        response = requests.get(url, headers=headers)
        print("API Response:")
        print(response.text)

        # Parsing JSON
        data = json.loads(response.text)
        print("JSON Data:")
        print(data)

        # Extract rankings list
        rankings_list = data.get('rankings', [])
        competitor_rankings = rankings_list[0].get('competitor_rankings', []) if rankings_list else []

        # Lists to store data
        competitor_data = []
        competitor_rankings_data = []

        # Process rankings
        for ranking in competitor_rankings:
            competitor = ranking.get('competitor', {})

            # Extract competitor details
            competitor_id = competitor.get('id')
            competitor_name = competitor.get('name')
            competitor_country = competitor.get('country')
            competitor_country_code = competitor.get('country_code')
            competitor_abbreviation = competitor.get('abbreviation')

            # Store competitor data
            competitor_data.append([
                competitor_id, competitor_name, competitor_country,
                competitor_country_code, competitor_abbreviation
            ])

            # Extract ranking details
            competitor_rank = ranking.get('rank')
            competitor_movement = ranking.get('movement')
            competitor_points = ranking.get('points')
            competitions_played = ranking.get('competitions_played')

            # Store ranking data
            competitor_rankings_data.append([
                competitor_rank, competitor_movement, competitor_points,
                competitions_played, competitor_id
            ])

        # Convert to DataFrames
        df_competitors = pd.DataFrame(competitor_data, columns=[
            'competitor_id', 'competitor_name', 'competitor_country',
            'competitor_country_code', 'competitor_abbreviation'
        ])
        df_rankings = pd.DataFrame(competitor_rankings_data, columns=[
            'competitor_rank', 'competitor_movement', 'competitor_points',
            'competitions_played', 'competitor_id'
        ])

        # Remove duplicates
        df_competitors.drop_duplicates(inplace=True)
        df_rankings.drop_duplicates(inplace=True)

        # Debugging: Print DataFrames
        print("Competitor Data:")
        print(df_competitors.head())
        print("\nRanking Data:")
        print(df_rankings.head())

        # Check if data is empty before inserting into MySQL
        if df_competitors.empty or df_rankings.empty:
            print("Error: DataFrames are empty, no data to insert into MySQL.")
            return

        with engine.begin() as conn:
            # Insert or Update Competitor Data
            competitor_sql = text("""
                INSERT INTO competitor_data (
                    competitor_id, competitor_name, competitor_country,
                    competitor_country_code, competitor_abbreviation
                ) VALUES (
                    :competitor_id, :competitor_name, :competitor_country,
                    :competitor_country_code, :competitor_abbreviation
                ) ON DUPLICATE KEY UPDATE 
                    competitor_name = VALUES(competitor_name),
                    competitor_country = VALUES(competitor_country),
                    competitor_country_code = VALUES(competitor_country_code),
                    competitor_abbreviation = VALUES(competitor_abbreviation);
            """)
            competitor_data_dicts = df_competitors.to_dict(orient='records')
            conn.execute(competitor_sql, competitor_data_dicts)

            # Insert or Update Rankings Data
            rankings_sql = text("""
                INSERT INTO competitor_rankings_data (
                    competitor_rank, competitor_movement, competitor_points,
                    competitions_played, competitor_id
                ) VALUES (
                    :competitor_rank, :competitor_movement, :competitor_points,
                    :competitions_played, :competitor_id
                ) ON DUPLICATE KEY UPDATE 
                    competitor_movement = VALUES(competitor_movement),
                    competitor_points = VALUES(competitor_points),
                    competitions_played = VALUES(competitions_played);
            """)
            rankings_data_dicts = df_rankings.to_dict(orient='records')
            conn.execute(rankings_sql, rankings_data_dicts)

            print("Data inserted successfully into MySQL!")

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
