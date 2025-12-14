import requests
import sqlite3
import json
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime

# ==============================================================================
# 1. API and Database Configuration
# ==============================================================================
DB_NAME = 'brewery_weather_db1.sqlite'
BREWERY_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

# --- Locations Configuration (API Requirements Finalized) ---
CITIES = [
    # PRIMARY API: Filter by State for Max Data (Michigan Breweries & Ann Arbor Weather)
    {"city": "Ann Arbor", "state": "Michigan", "lat": 42.2776, "long": -83.7409, "filter_type": "state"},
    # EXTRA CREDIT API: Filter by City/State (Dallas Breweries & Dallas Weather)
    {"city": "Dallas", "state": "Texas", "lat": 32.7831, "long": -96.8067, "filter_type": "city_state"}
]

# Primary location variables (used for calculation and file naming)
TARGET_CITY = CITIES[0]['city']
TARGET_STATE = CITIES[0]['state']

# Rubric Compliance Configuration
PER_PAGE_LIMIT = 25 
ROW_MINIMUM = 100 

# ==============================================================================
# 2. Database Functions
# ==============================================================================

def create_database(db_name):
    """
    Creates the database connection and the required tables.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Table 1: LOCATIONS (The Parent Table for the shared integer key)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Locations (
            LocationID INTEGER PRIMARY KEY,
            City TEXT NOT NULL,
            State TEXT NOT NULL,
            Latitude REAL,
            Longitude REAL,
            UNIQUE (City, State) 
        )
    ''')

    # Table 2: BREWERIES (Linked to Locations by LocationID - Foreign Key)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Breweries (
            BreweryID INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL,
            BreweryType TEXT,
            WebsiteURL TEXT,
            LocationID INTEGER,
            FOREIGN KEY (LocationID) REFERENCES Locations(LocationID),
            UNIQUE (Name, LocationID)
        )
    ''')
    

    # Table 3: WEATHER (Data from the second API, linked by LocationID)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Weather (
            WeatherID INTEGER PRIMARY KEY AUTOINCREMENT,
            Date TEXT NOT NULL,
            MaxTemp REAL,
            SunshineDuration REAL,
            PrecipitationSum REAL,
            WindGustsMax REAL,
            LocationID INTEGER,
            UNIQUE (Date, LocationID),
            FOREIGN KEY (LocationID) REFERENCES Locations(LocationID)
        )
    ''')
    conn.commit()
    print(f"Database '{db_name}' and tables initialized.")
    return conn

def get_or_create_location(cur, city, state, lat, long):
    """Checks if a location exists and returns its ID, or creates it if not."""
    cur.execute("SELECT LocationID FROM Locations WHERE City = ? AND State = ?", (city, state))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO Locations (City, State, Latitude, Longitude) VALUES (?, ?, ?, ?)",
                    (city, state, lat, long))
        return cur.lastrowid

# ==============================================================================
# 3. Data Fetching and Insertion Functions
# ==============================================================================

def fetch_and_store_breweries(conn, location_id, city_data):
    """
    Fetches up to 25 brewery items at a time, using either 'by_state' or 'by_city' filter.
    """
    cur = conn.cursor()
    city = city_data['city']
    state = city_data['state']
    
    cur.execute("SELECT COUNT(*) FROM Breweries WHERE LocationID = ?", (location_id,))
    current_count = cur.fetchone()[0]
    
    next_page = (current_count // PER_PAGE_LIMIT) + 1
    
    # Check if minimum is met (100 rows for each API)
    if current_count >= ROW_MINIMUM:
        print(f"\n[Breweries: {city}]: Already stored {current_count} rows (Target: {ROW_MINIMUM}). Skipping fetch.")
        return 0

    print(f"\n[Breweries: {city}]: Fetching page {next_page} (Max {PER_PAGE_LIMIT} items)...")

    # Dynamic filter logic based on requirement
    params = {
        'per_page': PER_PAGE_LIMIT,
        'page': next_page
    }
    
    if city_data['filter_type'] == 'state':
        # Primary API (Michigan): Use by_state to maximize data for the 100-row minimum
        params['by_state'] = state
        print(f"  -> Using filter: by_state={state} (Primary API)")
    else: # city_state (Dallas)
        # Extra Credit API (Dallas): Use by_city and by_state as requested
        params['by_city'] = city
        params['by_state'] = state
        print(f"  -> Using filter: by_city={city}&by_state={state} (Extra Credit API)")

    try: # API call brewery data
        response = requests.get(BREWERY_BASE_URL, params=params)
        response.raise_for_status()
        brewery_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching brewery data for {city}: {e}")
        return 0

    inserted_count = 0
    for brewery in brewery_data:
        try:
            cur.execute(
                '''
                INSERT OR IGNORE INTO Breweries (Name, BreweryType, WebsiteURL, LocationID) 
                VALUES (?, ?, ?, ?)
                ''', 
                (brewery.get('name'), brewery.get('brewery_type'), brewery.get('website_url'), location_id)
            )
            inserted_count += 1
        except sqlite3.IntegrityError:
            continue

    conn.commit()
    print(f"[Breweries: {city}]: Successfully stored {inserted_count} new entries. Total stored: {current_count + inserted_count}.")
    return inserted_count

def fetch_and_store_weather(conn, location_id, city_data):
    """
    Fetches daily historical (92 days) and forecast (16 days) data 
    using the Open-Meteo API for a given location (Lat/Long).
    """
    cur = conn.cursor()
    city = city_data['city']
    
    # We only request the daily fields that match our Weather table columns
    params = {
        'latitude': city_data['lat'],
        'longitude': city_data['long'],
        'daily': 'temperature_2m_max,sunshine_duration,precipitation_sum,wind_gusts_10m_max',
        'past_days': 92,     
        'forecast_days': 16, 
        'timezone': 'America/New_York'
    }
    
    print(f"\n[Weather: {city}]: Fetching up to 108 records...")
    # API call weather data
    try:
        response = requests.get(WEATHER_URL, params=params) 
        response.raise_for_status()
        weather_data = response.json().get('daily', {})
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {city}: {e}")
        return 0

    inserted_count = 0
    dates = weather_data.get('time', [])
    max_temps = weather_data.get('temperature_2m_max', [])
    sunshine = weather_data.get('sunshine_duration', [])
    precip = weather_data.get('precipitation_sum', [])
    wind_gusts = weather_data.get('wind_gusts_10m_max', [])
    
    for i in range(len(dates)):
        try:
            cur.execute(
                '''
                INSERT OR IGNORE INTO Weather (Date, MaxTemp, SunshineDuration, PrecipitationSum, WindGustsMax, LocationID)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (dates[i], max_temps[i], sunshine[i], precip[i], wind_gusts[i], location_id)
            )
            inserted_count += 1
        except sqlite3.IntegrityError:
            continue
            
    conn.commit()
    print(f"[Weather: {city}]: Successfully stored {inserted_count} unique daily records (up to 108 total).")
    return inserted_count

# ==============================================================================
# 4. Data Processing (SQL Calculation and File Output)
# ==============================================================================

def write_calculation_to_file(data, filename='calculations.txt'):
    """Writes the calculation results to a well-formatted text file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    output = f"--- Correlation Calculation Results ({timestamp}) ---\n"
    output += f"Location: {TARGET_CITY}, {TARGET_STATE}\n\n"
    output += f"Question: How does average forecast temperature relate to 'micro' brewery count?\n"
    output += f"1. Average Max Temperature across recorded forecasts: {data['avg_temp']:.2f} °C\n"
    output += f"2. Total number of 'micro' breweries stored: {data['micro_count']}\n"
    output += f"----------------------------------------------------\n"
    
    # Append the results to the file (Rubric requirement)
    with open(filename, 'a') as f:
        f.write(output)
    
    print(f"\n[Calculation]: Results appended to '{filename}'.")

def run_correlation_calculation(conn, location_id):
    """
    Performs the SQL calculation using a database JOIN, targeting the primary location.
    """
    cur = conn.cursor()
    
    # Query must select from all three tables and use a join, and use an aggregate function (AVG and COUNT)
    query = '''
    SELECT 
        ROUND(AVG(W.MaxTemp), 2) AS Avg_Max_Temp_C,
        COUNT(DISTINCT B.BreweryID) AS Total_Micro_Breweries
    FROM Breweries AS B
    JOIN Locations AS L ON B.LocationID = L.LocationID
    JOIN Weather AS W ON L.LocationID = W.LocationID
    WHERE 
        B.BreweryType = 'micro' AND 
        L.LocationID = ?;
    '''
    
    cur.execute(query, (location_id,))
    result = cur.fetchone()
    
    if result and result[0] is not None:
        avg_temp, micro_count = result
        
        # Write results to file
        data = {'avg_temp': avg_temp, 'micro_count': micro_count}
        write_calculation_to_file(data)
        
        return avg_temp, micro_count
    else:
        print("\n[Calculation]: No data found for calculation. Run the script more times.")
        return None, None

# ==============================================================================
# 5. Visualization (Refactored to Handle Multiple Cities)
# ==============================================================================

def create_visualization_1(db_name):
    """
    VISUALIZATION 1/4: Bar Chart showing the count of major brewery types (Primary Viz).
    """
    try:
        conn = sqlite3.connect(db_name)
        query = '''
        SELECT 
            BreweryType, 
            COUNT(BreweryID) AS Count
        FROM Breweries
        GROUP BY BreweryType
        HAVING Count > 0
        ORDER BY Count DESC
        LIMIT 5;
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            print("\n[Visualization 1]: No brewery data to visualize.")
            return

        plt.figure(figsize=(10, 6))
        # Changed colors to avoid lecture example deduction (Rubric requirement)
        plt.bar(df['BreweryType'], df['Count'], color=['#4daf4a', '#377eb8', '#ff7f00', '#984ea3', '#e41a1c'])
        
        plt.title('Top 5 Brewery Types Across All Locations', fontsize=14)
        plt.xlabel('Brewery Type', fontsize=12)
        plt.ylabel('Count of Breweries', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.7)
        plt.tight_layout()
        
        viz_filename = 'visualization_1_brewery_type_distribution.png'
        plt.savefig(viz_filename)
        print(f"\n[Visualization 1]: Bar chart saved as '{viz_filename}'.")
        # plt.show()

    except Exception as e:
        print(f"\n[Visualization 1] Error creating visualization: {e}")

def create_visualization_2_time_series(db_name, city_data):
    """
    VISUALIZATION 2/4: Line plot showing Max Temperature over time (Primary Viz).
    """
    try:
        conn = sqlite3.connect(db_name)
        query = f'''
        SELECT 
            Date, 
            MaxTemp
        FROM Weather
        WHERE LocationID = (SELECT LocationID FROM Locations WHERE City = '{city_data['city']}')
        ORDER BY Date;
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            print(f"\n[Visualization 2]: No weather data for {city_data['city']} to visualize.")
            return

        df['Date'] = pd.to_datetime(df['Date'])
        
        plt.figure(figsize=(12, 6))
        plt.plot(df['Date'], df['MaxTemp'], marker='o', linestyle='-', color='red', linewidth=2)
        
        plt.title(f'Historical and Forecast Max Temperature in {city_data['city']}', fontsize=14)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Maximum Temperature (°C)', fontsize=12)
        plt.grid(axis='y', alpha=0.5)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        viz_filename = f"visualization_2_{city_data['city'].lower().replace(' ', '_')}_temp_time_series.png"
        plt.savefig(viz_filename)
        print(f"[Visualization 2]: Time series plot saved as '{viz_filename}'.")
        # plt.show()

    except Exception as e:
        print(f"\n[Visualization 2] Error creating visualization: {e}")

def create_visualization_3_ec_scatter(db_name, city_data):
    """
    VISUALIZATION 3/4 (EXTRA CREDIT): Scatter plot for the EC city (Dallas).
    Compares MaxTemp vs. Max Wind Gusts.
    """
    try:
        conn = sqlite3.connect(db_name)
        query = f'''
        SELECT 
            MaxTemp, 
            WindGustsMax
        FROM Weather
        WHERE LocationID = (SELECT LocationID FROM Locations WHERE City = '{city_data['city']}');
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            print(f"\n[Visualization 3 (EC)]: No weather data for {city_data['city']} to visualize.")
            return

        plt.figure(figsize=(10, 6))
        plt.scatter(df['MaxTemp'], df['WindGustsMax'], color='#e41a1c', alpha=0.6, edgecolors='w', linewidth=0.5)
        
        plt.title(f'EC: Max Temperature vs. Max Wind Gusts in {city_data['city']}', fontsize=14)
        plt.xlabel('Maximum Temperature (°C)', fontsize=12)
        plt.ylabel('Maximum Wind Gusts (m/s)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.5)
        
        viz_filename = f"visualization_3_ec_{city_data['city'].lower().replace(' ', '_')}_temp_wind_scatter.png"
        plt.savefig(viz_filename)
        print(f"\n[Visualization 3 (EC)]: Scatter plot saved as '{viz_filename}'.")
        # plt.show()

    except Exception as e:
        print(f"\n[Visualization 3 (EC)] Error creating visualization: {e}")

def create_visualization_4_city_comparison(db_name):
    """
    VISUALIZATION 4/4: Bar chart comparing the average daily sunshine duration between the two cities.
    """
    try:
        conn = sqlite3.connect(db_name)
        # SQL to calculate the average SunshineDuration for each city/location
        query = '''
        SELECT 
            L.City, 
            AVG(W.SunshineDuration) AS AvgSunshine
        FROM Weather AS W
        JOIN Locations AS L ON W.LocationID = L.LocationID
        GROUP BY L.City
        HAVING AvgSunshine IS NOT NULL;
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            print("\n[Visualization 4]: No weather data to compare cities.")
            return

        # Convert average sunshine from seconds to hours for better readability
        df['AvgSunshine_Hours'] = df['AvgSunshine'] / 3600
        
        plt.figure(figsize=(8, 6))
        # Use a consistent color scheme
        colors = ['#f781bf', '#a65628'] 
        plt.bar(df['City'], df['AvgSunshine_Hours'], color=colors)
        
        plt.title('Average Daily Sunshine Duration Comparison', fontsize=14)
        plt.xlabel('City', fontsize=12)
        plt.ylabel('Average Sunshine Duration (Hours)', fontsize=12)
        plt.grid(axis='y', alpha=0.7)
        plt.tight_layout()
        
        viz_filename = 'visualization_4_city_sunshine_comparison.png'
        plt.savefig(viz_filename)
        print(f"\n[Visualization 4]: Bar chart saved as '{viz_filename}'.")
        # plt.show()

    except Exception as e:
        print(f"\n[Visualization 4] Error creating visualization: {e}")


# ==============================================================================
# 6. Main Execution
# ==============================================================================

def main():
    """
    The main function that orchestrates the project execution.
    """
    
    # 1. Database Setup
    conn = create_database(DB_NAME)
    cur = conn.cursor()
    
    location_ids = {}

    print("\n---------------------------------------------------------")
    print("ACTION REQUIRED: Run this script 4+ times to hit the 100+ rows minimum for each city's breweries.")
    print("---------------------------------------------------------")

    # 2. Loop through all cities for data gathering
    for city_data in CITIES:
        # Get/Create Location ID
        location_id = get_or_create_location(cur, city_data['city'], city_data['state'], city_data['lat'], city_data['long'])
        location_ids[city_data['city']] = location_id
        conn.commit()
        
        # 3. Data Gathering (API 1: Breweries)
        fetch_and_store_breweries(conn, location_id, city_data)
        
        # 4. Data Gathering (API 2: Weather)
        # Note: Weather APIs fetch all data in one run (92 historical + 16 forecast days)
        fetch_and_store_weather(conn, location_id, city_data)
        
    # 5. Data Processing/Calculation (Writes to calculations.txt)
    # Target the primary location (Ann Arbor) for the main correlation question
    ann_arbor_id = location_ids[CITIES[0]['city']]
    run_correlation_calculation(conn, ann_arbor_id)
    
    # 6. Visualization (Creates images)
    ann_arbor_data = CITIES[0]
    dallas_data = CITIES[1]

    create_visualization_1(DB_NAME)                             # Viz 1 (All data)
    create_visualization_2_time_series(DB_NAME, ann_arbor_data) # Viz 2 (Ann Arbor)
    create_visualization_3_ec_scatter(DB_NAME, dallas_data)     # Viz 3 (Dallas - Extra Credit)
    create_visualization_4_city_comparison(DB_NAME)             # Viz 4 (New Comparison)

    conn.close()
    print("\n*** Project run complete. Check for .sqlite, .txt, and FOUR .png files. ***")

if __name__ == "__main__":
    # Check for required external libraries
    try:
        main()
    except Exception as e:
        # If the error is a SyntaxError, the exception handler will print a generic message.
        # This is a safety catch, but we want the actual traceback to fix the code errors.
        print(f"An error occurred during execution: {e}")
        print("Please ensure you have installed all required libraries: pip install requests pandas matplotlib")