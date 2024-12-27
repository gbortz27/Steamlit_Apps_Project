import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

# Set up the Snowflake connection
# Set up the Snowflake connection
connection_parameters = {"account":"jm52830.eu-west-1",
"user":"GRAHAM_BORTZ",
"password": "Greenham27",
"role":"CYBER_SEC_TESTING",
"warehouse":"ANALYSIS_VWH",
"database":"TEST_YMH",
"schema":"CYBER_SEC_TESTING"
}

# Create a session
session = Session.builder.configs(connection_parameters).create()

@st.cache_data(show_spinner=False)
def load_data():
    # Load data from Snowflake
    query = "SELECT * FROM DASHBOARD_DATA"
    data_df = session.sql(query).to_pandas()
    return data_df

# Load data once and cache it
data_df = load_data()

st.title("Sales Dashboard")

# Create columns for layout
top_left, top_right = st.columns(2)
bottom_left, bottom_right = st.columns(2)

# Show DataFrame on top left
with top_left:
    st.subheader("DataFrame")
    st.dataframe(data_df)

# Bar chart on top right
with top_right:
    st.subheader("Revenue by Category")
    bar_chart_data = data_df.groupby('category')['revenue'].sum().reset_index()
    st.bar_chart(bar_chart_data.set_index('category'))

# Line chart on bottom left
with bottom_left:
    st.subheader("Monthly Sales")
    line_chart_data = data_df.groupby('month')['sales'].sum().reindex([
        'January', 'February', 'March', 'April', 'May', 'June', 'July', 
        'August', 'September', 'October', 'November', 'December']).reset_index()
    st.line_chart(line_chart_data.set_index('month'))

# Table with slider filter on bottom right
with bottom_right:
    st.subheader("Filtered Table by Revenue")
    revenue_filter = st.slider("Select minimum revenue", 
                               min_value=int(data_df['revenue'].min()), 
                               max_value=int(data_df['revenue'].max()), 
                               value=int(data_df['revenue'].min()))
    filtered_data = data_df[data_df['revenue'] >= revenue_filter]
    st.dataframe(filtered_data)

