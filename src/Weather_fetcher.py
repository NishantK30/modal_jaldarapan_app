import requests
from datetime import timedelta,datetime
import pandas as pd
import numpy as np


def get_coordinates(location_name,api_key):
    
    """
    Fetch the latitude, longitude, and elevation of a location using OpenCage Geocoding API.
    
    Parameters:
        location_name (str): Name of the location.
    
    Returns:
        tuple: (latitude, longitude, elevation)
    """
    try:
        # OpenCage API key
        url = f"https://api.opencagedata.com/geocode/v1/json?q={location_name}&key={api_key}"
        
        # Make the API request
        response = requests.get(url)
        data = response.json()
        
        # Extract latitude, longitude, and elevation
        if data['results']:
            lat = data['results'][0]['geometry']['lat']
            lon = data['results'][0]['geometry']['lng']
            elevation = 0  # Defaulting to 0 (OpenCage doesn't provide elevation)
            return lat, lon, elevation
        else:
            print(f"Error: OpenCage could not find coordinates for {location_name}.")
            return None, None, None

    except Exception as e:
        print(f"An error occurred while fetching coordinates: {e}")
        return None, None, None
    
def fetch_weather_data(location_name,api_key,days=3):
    from meteostat import Point,Daily
    
    """
    Fetch weather data for the last n days for a given location.
    
    Parameters:
        location_name (str): Name of the location.
    
    Returns:
        pd.DataFrame: Weather data for the last two days.
    """
    try:
        # Fetch coordinates of the location
        latitude, longitude, elevation = get_coordinates(location_name,api_key)

        # If coordinates are not found, return an empty DataFrame
        if latitude is None or longitude is None:
            print(f"Could not fetch weather data for {location_name} due to missing coordinates.")
            return pd.DataFrame()
        
        # Define the location using the coordinates
        location = Point(latitude, longitude, elevation)

        # Define the date range: last two days
        end_date = datetime.now()
        start_date = end_date - timedelta(days)

        # Fetch daily weather data
        weather_data = Daily(location, start_date, end_date).fetch()
        
        # Check if data is available
        if weather_data.empty:
            print(f"No weather data available for {location_name}.")
            return None, np.array([])
        
        # Select only the desired columns and add location name
        processed_data = weather_data.reset_index()[['time', 'prcp']]
        processed_data['location'] = location_name
        
        # Reorder columns
        processed_data = processed_data[['time', 'location', 'prcp']]
        
        # Extract precipitation data into an array
        prcp_array = processed_data['prcp'].to_numpy()
        
        print(f"Weather data for {location_name} fetched successfully!")
        return processed_data, prcp_array
    
    except Exception as e:
        print(f"An error occurred while fetching weather data for {location_name}: {e}")
        return None, np.array([])