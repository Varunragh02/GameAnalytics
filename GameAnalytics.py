import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text

# Database connection setup
DB_URL = "mysql+mysqlconnector://root:Selvamk1403#@localhost/gameanalytics"
engine = create_engine(DB_URL)

# Function to load data from MySQL safely
@st.cache_data
def load_data(table_name):
    """Fetch data from MySQL table safely using SQLAlchemy."""
    try:
        if not isinstance(table_name, str) or not table_name.isidentifier():
            st.error("Invalid table name.")
            return pd.DataFrame()
        
        query = text(f"SELECT * FROM `{table_name}`")
        with engine.connect() as connection:
            df = pd.read_sql(query, con=connection)
        return df.astype(str)  # Convert to string to prevent dtype issues
    except Exception as e:
        st.error(f"Error loading {table_name}: {e}")
        return pd.DataFrame()

# 🎾 Dashboard Title
st.title("🎾 Tennis Data Dashboard")

# Sidebar Navigation
st.sidebar.title("📌 Navigation")
page = st.sidebar.selectbox(
    "Choose a section:",
    options=["🏠 Home", "🏆 Competitions", "📍 Venues", "📊 Rankings", "🔍 Search Competitor"]
)

# 🏠 Homepage - Summary Statistics
if page == "🏠 Home":
    st.header("🏠 Dashboard Summary")
    df_competitors = load_data("competitor_data")
    df_competitor_ranking = load_data("competitor_rankings_data")
    
    if not df_competitors.empty:
        total_competitors = len(df_competitors)
        total_countries = df_competitors['competitor_country'].nunique()
        highest_points = df_competitor_ranking['competitor_points'].max()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Competitors", total_competitors)
        with col2:
            st.metric("Countries Represented", total_countries)
        with col3:
            st.metric("Highest Points Scored", highest_points)
        
        # 🌍 Map Visualization - Competitor Distribution by Country
        st.subheader("🌍 Competitor Distribution Map")
        country_counts = df_competitors['competitor_country'].value_counts().reset_index()
        country_counts.columns = ["Country", "Competitor Count"]
        
        fig_map = px.choropleth(
            country_counts,
            locations="Country",
            locationmode="country names",
            color="Competitor Count",
            title="Competitor Distribution Across Countries",
            color_continuous_scale="Viridis"
        )
        st.plotly_chart(fig_map)
    else:
        st.write("No data available.")

# 🏆 Competitions
elif page == "🏆 Competitions":
    st.header("🏆 Tennis Competitions")
    df_competitions = load_data("competition_data")
    
    if not df_competitions.empty:
        st.dataframe(df_competitions)
        
        if "competition_type" in df_competitions.columns:
            competition_counts = df_competitions["competition_type"].value_counts().reset_index()
            competition_counts.columns = ["Competition Type", "Count"]
            fig = px.bar(competition_counts, x="Competition Type", y="Count", title="🎾 Competitions by Type")
            st.plotly_chart(fig)
    else:
        st.write("No competition data available.")

# 📍 Venues
elif page == "📍 Venues":
    st.header("📍 Tennis Venues")
    df_venues = load_data("venues_data")
    
    if not df_venues.empty:
        st.dataframe(df_venues)
        
        if "country_name" in df_venues.columns:
            venue_counts = df_venues["country_name"].value_counts().reset_index()
            venue_counts.columns = ["Country", "Count"]
            fig = px.pie(venue_counts, names="Country", values="Count", title="🎾 Venue Distribution by Country")
            st.plotly_chart(fig)
    else:
        st.write("No venue data available.")

# 📊 Rankings
elif page == "📊 Rankings":
    st.header("📊 Player Rankings")
    df_rankings = load_data("competitor_rankings_data")
    df_competitors = load_data("competitor_data")
    
    if not df_rankings.empty and not df_competitors.empty:
        df_rankings = df_rankings.merge(df_competitors, on="competitor_id", how="left")
        st.dataframe(df_rankings)
        
        if "competitor_name" in df_rankings.columns and "competitor_points" in df_rankings.columns:
            top_players = df_rankings.sort_values(by="competitor_rank").head(10)
            fig = px.bar(
                top_players, x="competitor_name", y="competitor_points",
                title="🏆 Top 10 Players by Points", color="competitor_name"
            )
            st.plotly_chart(fig)
    else:
        st.write("No ranking data available.")

# 🔍 Search Competitor
elif page == "🔍 Search Competitor":
    st.header("🔍 Search for a Competitor")
    df_competitors = load_data("competitor_data")
    df_rankings = load_data("competitor_rankings_data")
    
    if not df_competitors.empty and not df_rankings.empty:
        df_merged = df_rankings.merge(df_competitors, on="competitor_id", how="left")
        competitor_names = df_merged["competitor_name"].dropna().unique()
        selected_name = st.selectbox("Select or Search Competitor", sorted(competitor_names))
        
        if selected_name:
            competitor_data = df_merged[df_merged["competitor_name"] == selected_name]
            
            if not competitor_data.empty:
                st.subheader(f"🎾 {selected_name}'s Profile")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Country:** {competitor_data.iloc[0]['competitor_country']}")
                    st.write(f"**Abbreviation:** {competitor_data.iloc[0]['competitor_abbreviation']}")
                with col2:
                    st.write(f"**Current Rank:** {competitor_data.iloc[0]['competitor_rank']}")
                    st.write(f"**Points:** {competitor_data.iloc[0]['competitor_points']}")
                
                # 📊 Ranking Trend
                st.subheader("📊 Ranking Progression")
                fig_rank = px.line(
                    competitor_data.sort_values(by="competitor_rank"),
                    x="competitor_rank", y="competitor_points",
                    markers=True, title=f"{selected_name}'s Ranking Trend"
                )
                st.plotly_chart(fig_rank)
    else:
        st.write("No data available.")

st.sidebar.write("Developed using Streamlit and MySQL")
