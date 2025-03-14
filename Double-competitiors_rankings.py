import streamlit as st
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import plotly.express as px

# Database connection setup
DB_URL = "mysql+mysqlconnector://root:Selvamk1403#@localhost/gameanalytics"
engine = create_engine(DB_URL)

def load_data(table_name, engine):
    """Fetch data from MySQL table."""
    try:
        query = "SELECT * FROM {}".format(table_name)  # Correct format usage
        return pd.read_sql(query, con=engine)
    except Exception as e:
        st.error("Error loading {}: {}".format(table_name, e))
        return pd.DataFrame()

# Load all datasets
st.title("🎾 Tennis Data Dashboard")
st.write("Explore various datasets related to tennis competitions, venues, and rankings.")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Competitions", "Venues", "Rankings", "Search Competitor"])

if page == "Competitions":
    st.header("🏆 Tennis Competitions")
    df_competitions = load_data("competition_data")
    st.dataframe(df_competitions, use_container_width=True)

elif page == "Venues":
    st.header("📍 Tennis Venues")
    df_venues = load_data("venues_data")
    st.dataframe(df_venues)

elif page == "Rankings":
    st.header("📊 Player Rankings")
    df_rankings = load_data("competitor_rankings_data")
    df_competitors = load_data("competitor_data")

    if not df_rankings.empty and not df_competitors.empty:
        df = df_rankings.merge(df_competitors, on="competitor_id", how="left")
        
        # Rank Range Filter
        min_rank, max_rank = st.sidebar.slider("Select Rank Range", 1, 100, (1, 10))
        df = df[(df["competitor_rank"] >= min_rank) & (df["competitor_rank"] <= max_rank)]
        
        st.write("### Filtered Rankings")
        st.dataframe(df[['competitor_rank', 'competitor_name', 'competitor_country', 'competitor_points']])

        fig = px.bar(df, x="competitor_name", y="competitor_points", 
                     title="Players by Points", color="competitor_points")
        st.plotly_chart(fig)

elif page == "Search Competitor":
    st.header("🔍 Search Competitor")
    search_name = st.text_input("Enter Competitor Name")
    
    if search_name:
        df_competitors = load_data("competitor_data")
        df_rankings = load_data("competitor_rankings_data")
        df_competitions = load_data("competition_data")
        
        df = df_rankings.merge(df_competitors, on="competitor_id", how="left")
        df = df[df["competitor_name"].str.contains(search_name, case=False, na=False)]
        
        if not df.empty:
            st.write("### Competitor Details")
            
            for _, row in df.iterrows():
               col1, col2, col3 = st.columns(3)
               col1.write(f"🏆 **Name:** {row['competitor_name']}")
               col2.write(f"🌍 **Country:** {row['competitor_country']}")
               col3.write(f"📊 **Rank:** {row['competitor_rank']}")
               
               col1.write(f"🔥 **Points:** {row['competitor_points']}")
               col2.write(f"📅 **Competitions Played:** {row['competitions_played']}")
               col3.write(f"🔄 **Movement:** {row['competitor_movement']}")
        
               st.markdown("---")  # Adds a horizontal separator
        else:
            st.write("No competitor found with that name.")

st.sidebar.write("Developed with ❤️ using Streamlit and MySQL")
