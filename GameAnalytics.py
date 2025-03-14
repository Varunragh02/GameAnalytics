import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# Database connection setup
DB_URL = "mysql+mysqlconnector://root:Selvamk1403#@localhost/gameanalytics"
engine = create_engine(DB_URL)

# Function to load data from MySQL
@st.cache_data
def load_data(table_name, engine):
    """Fetch data from MySQL table."""
    try:
        query = "SELECT * FROM {}".format(table_name)  # Correct format usage
        return pd.read_sql(query, con=engine)
    except Exception as e:
        st.error("Error loading {}: {}".format(table_name, e))
        return pd.DataFrame()

# 🎾 Dashboard Title
st.title("🎾 Tennis Data Dashboard")

# Sidebar Navigation
st.sidebar.title("📌 Navigation")
page = st.sidebar.selectbox(
    "Choose a section:",
    options=["🏠 Home", "🏆 Competitions", "📍 Venues", "📊 Rankings", "🔍 Search Competitor"]
)

# 🏠 Home
if page == "🏠 Home":
    st.header("🏠 Welcome to the Tennis Dashboard!")
    st.write("Use the sidebar to navigate through the different sections.")

# 🏆 Competitions
elif page == "🏆 Competitions":
    st.header("🏆 Tennis Competitions")
    df_competitions = load_data("competition_data")
    
    if not df_competitions.empty:
        st.dataframe(df_competitions)
        
        # Bar chart for competitions by type
        competition_counts = df_competitions["competition_type"].value_counts().reset_index()
        competition_counts.columns = ["Competition Type", "Count"]
        fig = px.bar(competition_counts, x="Competition Type", y="Count", title="🎾 Competitions by Type")
        st.plotly_chart(fig)

# 📍 Venues
elif page == "📍 Venues":
    st.header("📍 Tennis Venues")
    df_venues = load_data("venues_data")
    
    if not df_venues.empty:
        st.dataframe(df_venues)
        
        # Pie chart for venue distribution by country
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
    df_competitors = load_data("competitor_data")  # Load competitor data
    df_rankings = df_rankings.merge(df_competitors, on="competitor_id", how="left")  # Merge to get competitor_name
    
    if not df_rankings.empty:
        st.dataframe(df_rankings)

        # Bar chart for top 10 players
        top_players = df_rankings.sort_values(by="competitor_rank").head(10)
        fig = px.bar(
            top_players, x="competitor_name", y="competitor_points",  # Use competitor_name for X-axis
            title="🏆 Top 10 Players by Points", color="competitor_name"
        )
        st.plotly_chart(fig)

# 🔍 Search Competitor
elif page == "🔍 Search Competitor":
    st.header("🔍 Search Competitor")

    # Load data
    df_competitors = load_data("competitor_data")
    df_rankings = load_data("competitor_rankings_data")

    # Get unique competitor names for dropdown
    competitor_list = df_competitors["competitor_name"].dropna().unique().tolist()

    # Search box with dropdown
    search_name = st.selectbox("Select a Competitor", [""] + competitor_list)

    if search_name:
        # Merge and filter data for selected competitor
        df_selected = df_rankings.merge(df_competitors, on="competitor_id", how="left")
        df_selected = df_selected[df_selected["competitor_name"] == search_name]

        if not df_selected.empty:
            # Display competitor details
            st.subheader("🏆 Competitor Details")
            st.write(f"**Name:** {search_name}")
            st.write(f"**Country:** {df_selected.iloc[0]['competitor_country']}")
            st.write(f"**Points:** {df_selected.iloc[0]['competitor_points']}")
            st.write(f"**Competitions Played:** {df_selected.iloc[0]['competitions_played']}")

            # 📊 **Bar Chart - Points per Competition**
            st.subheader("🏆 Points per Competition")
            fig_bar = px.bar(df_selected, x="competitions_played", y="competitor_points",
                             title="Points Earned per Competition", color="competitor_points")
            st.plotly_chart(fig_bar)

            # 📈 **Area Chart - Points Growth**
            st.subheader("📈 Points Growth Over Time")
            fig_area = px.area(df_selected, x=df_selected.index, y="competitor_points",
                               title="Points Growth Over Time", markers=True)
            st.plotly_chart(fig_area)

            # 🥇 **Pie Chart - Win Rate (if available)**
            if "win_rate" in df_selected.columns:
                st.subheader("🍕 Win Rate Distribution")
                fig_pie = px.pie(df_selected, names="win_rate", values="competitor_points",
                                 title="Win Rate Contribution to Points")
                st.plotly_chart(fig_pie)

        else:
            st.write("No details found for the selected competitor.")

st.sidebar.write("Developed with ❤️ using Streamlit and MySQL")
