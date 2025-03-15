import requests
import json
import pandas as pd
import mysql.connector
import schedule
import time
from sqlalchemy import create_engine, String
from sqlalchemy.sql import text

# API URL and headers
url = "https://api.sportradar.com/tennis/trial/v3/en/competitions.json?api_key=3dUQ9pQLy122TLHBCb7eInTMSRRHBh56RqYciW8b"
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

        # Extracting data from JSON
        competitions = data.get('competitions', [])
        competition_data = []
        category_data = []

        # Iterating through competitions
        for competition in competitions:
            competition_id = competition.get('id', None)
            parentid = competition.get('parent_id', None)  # Corrected key
            competition_name = competition.get('name', None)
            competition_type = competition.get('type', None)
            gender = competition.get('gender', None)

            # Extract category details
            category = competition.get('category', {})
            category_id = category.get('id', None)
            category_name = category.get('name', None)

            # Store data in lists
            competition_data.append([competition_id, parentid, competition_name, competition_type, gender, category_id])
            category_data.append([category_id, category_name])

        # Convert to Pandas DataFrame
        df_competition = pd.DataFrame(competition_data, columns=['competition_id', 'parentid', 'competition_name', 'competition_type', 'gender', 'category_id'])
        df_category = pd.DataFrame(category_data, columns=['category_id', 'category_name'])

        # Remove duplicates
        df_competition.drop_duplicates(inplace=True)
        df_category.drop_duplicates(inplace=True)

        # Debugging: Print DataFrames before inserting into MySQL
        print("Competition Data:")
        print(df_competition.head())
        print("\nCategory Data:")
        print(df_category.head())

        with engine.begin() as conn:
            try:
                # Convert lists to tuples and Insert or Update Category Data
                category_sql = text("""
                    INSERT INTO category_data (category_id, category_name)
                    VALUES (:category_id, :category_name)
                    ON DUPLICATE KEY UPDATE category_name = VALUES(category_name);
                """)
                category_data_dicts = df_category.to_dict(orient='records')
                conn.execute(category_sql, category_data_dicts)

                # Convert lists to tuples and Insert Competition Data
                competition_sql = text("""
                    INSERT INTO competition_data (competition_id, parent_id, competition_name, competition_type, gender, category_id)
                    VALUES (:competition_id, :parent_id, :competition_name, :competition_type, :gender, :category_id)
                    ON DUPLICATE KEY UPDATE 
                        parent_id = VALUES(parent_id),
                        competition_name = VALUES(competition_name),
                        competition_type = VALUES(competition_type),
                        gender = VALUES(gender),
                        category_id = VALUES(category_id);
                """)
                competition_data_dicts = [
                    {
                        "competition_id": row[0], "parent_id": row[1], "competition_name": row[2],
                        "competition_type": row[3], "gender": row[4], "category_id": row[5]
                    } for row in df_competition.values
                ]
                conn.execute(competition_sql, competition_data_dicts)

                print(" Data inserted successfully into MySQL with foreign key constraint!")

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
        time.sleep(60)  # Check every minute