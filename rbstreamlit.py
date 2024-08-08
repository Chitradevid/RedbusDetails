import streamlit as st
import pandas as pd
import pymysql

# Database connection function
def connect_to_mysql():
    return pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='2210',
        database='finalredbusproject'
    )

# Fetch distinct values for a given column
def fetch_distinct_values(connection, column_name):
    query = f"SELECT DISTINCT {column_name} FROM final_rb"
    return pd.read_sql(query, connection)

# Convert timedelta to HH:MM:SS format
def convert_timedelta_to_hhmmss(td):
    if pd.isna(td):
        return None
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

# Fetch filtered data from MySQL 
def fetch_filtered_data(connection, bustype, route, min_price, max_price, min_rating, max_rating):
    # Base query
    query = f"""
    SELECT route_name, route_link, busname, bustype, departing_time, duration, reaching_time, star_rating, price, seats_available 
    FROM final_rb
    WHERE 1=1
    """
    
    # List to collect conditions
    conditions = []
    params = []

    # Append conditions based on filter selections
    if bustype != "All":
        conditions.append("bustype = %s")
        params.append(bustype)
    if route != "All":
        conditions.append("route_name = %s")
        params.append(route)
    conditions.append("price BETWEEN %s AND %s")
    params.extend([min_price, max_price])
    conditions.append("star_rating BETWEEN %s AND %s")
    params.extend([min_rating, max_rating])

    # Combine conditions
    if conditions:
        query += " AND " + " AND ".join(conditions)
    
    df = pd.read_sql(query, connection, params=params)
    
    # Convert time columns from Timedelta to HH:MM:SS format
    df['departing_time'] = df['departing_time'].apply(lambda x: convert_timedelta_to_hhmmss(pd.Timedelta(x)))
    df['duration'] = df['duration'].apply(lambda x: convert_timedelta_to_hhmmss(pd.Timedelta(x)))
    df['reaching_time'] = df['reaching_time'].apply(lambda x: convert_timedelta_to_hhmmss(pd.Timedelta(x)))
    
    return df

# Streamlit app
def main():
    st.title("Red Bus Details View")
    
    # Connect to the database
    connection = connect_to_mysql()
    
    # Fetch distinct values for filters
    bustypes = fetch_distinct_values(connection, 'bustype')['bustype'].tolist()
    routes = fetch_distinct_values(connection, 'route_name')['route_name'].tolist()
    
    # Add "All" option to the lists
    bustypes.insert(0, "All")
    routes.insert(0, "All")
    
    # Sidebar filters
    selected_bustype = st.sidebar.selectbox("Bus Type", bustypes)
    selected_route = st.sidebar.selectbox("Route", routes)
    min_price, max_price = st.sidebar.slider("Price Range", 0, 6000, (0, 6000))
    min_rating, max_rating = st.sidebar.slider("Star Rating Range", 0.0, 5.0, (0.0, 5.0), step=0.2)
    
    # Submit button
    if st.sidebar.button("Submit"):
        # Fetch filtered data
        df = fetch_filtered_data(connection, selected_bustype, selected_route, min_price, max_price, min_rating, max_rating)
        
        # Display the data in a table
        st.write("Buses")
        st.dataframe(df)
    
    # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()
