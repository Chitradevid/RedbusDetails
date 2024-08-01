import streamlit as st
import pymysql
import pandas as pd

def get_connection():
    return pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='2210',
        db='redbusproject'
    )

def fetch_data(query):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return pd.DataFrame(result)
    finally:
        connection.close()

st.title("RedBus Data")

# Fetch the date
date_query = "SELECT DISTINCT Date FROM redbusdetails"
date = fetch_data(date_query)['Date'].tolist()[0]  # Assuming there is only one distinct date

# Queries to fetch distinct bus types and routes
bustype_query = "SELECT DISTINCT bustype FROM redbusdetails"
route_query = "SELECT DISTINCT route_name FROM redbusdetails"

# Fetch bus types and routes
bustypes = fetch_data(bustype_query)['bustype'].tolist()
routes = fetch_data(route_query)['route_name'].tolist()

# Display the date and filter options
st.header(f"Date: {date}")
st.subheader("Filter options")

selected_bustype = st.selectbox("Bus Type", ["All"] + bustypes)
selected_route = st.selectbox("Route", ["All"] + routes)
price_range = st.slider("Price Range", 0, 5000, (0, 5000))
min_rating = st.slider("Minimum Rating", 0.0, 5.0, 0.0)
available_seats = st.slider("Minimum Seats Available", 0, 50, 0)

# Add a submit button
submit_button = st.button("Submit")

if submit_button:
    # Base query
    query = "SELECT Transport, route_name, route_link, Date, busname, bustype, departing_time, duration, bp_time, star_rating, price, seats_available FROM redbusdetails WHERE 1=1"
    
    # List to collect conditions
    conditions = []

    # Append conditions based on filter selections
    if selected_bustype != "All":
        conditions.append(f"bustype = '{selected_bustype}'")
    if selected_route != "All":
        conditions.append(f"route_name = '{selected_route}'")
    conditions.append(f"price BETWEEN {price_range[0]} AND {price_range[1]}")
    conditions.append(f"star_rating >= {min_rating}")
    conditions.append(f"seats_available >= {available_seats}")

    # Join conditions with 'AND' and append to the base query
    if conditions:
        query += " AND " + " AND ".join(conditions)

    # Fetch and display data
    data = fetch_data(query)
    st.write("Result")
    st.dataframe(data)
