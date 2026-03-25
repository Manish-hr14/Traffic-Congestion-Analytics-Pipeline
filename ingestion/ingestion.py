import requests
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


LOCATIONS = [
    (28.7041, 77.1025),  # Delhi Center
    (28.5355, 77.3910),  # Noida
    (28.4595, 77.0266),  # Gurgaon
    (28.6692, 77.4538),  # Ghaziabad
    (28.4089, 77.3178),  # Faridabad
    (28.6139, 77.2090),  # Connaught Place
    (28.6507, 77.2334),  # Karol Bagh
    (28.5672, 77.2100),  # AIIMS / South Delhi
    (28.5245, 77.1855),  # Saket
    (28.5921, 77.0460),  # Dwarka
    (28.6280, 77.3649),  # Sector 62 Noida
    (28.4080, 77.3170),  # Faridabad Industrial
    (28.6760, 77.3210),  # East Delhi
    (28.7328, 77.1200),  # Rohini
    (28.7485, 77.0560),  # Pitampura
    (28.6690, 77.1000),  # West Delhi
    (28.5500, 77.2500),  # Lajpat Nagar
    (28.6200, 77.3000),  # Mayur Vihar
    (28.4800, 77.0800),  # Gurgaon Cyber City
    (28.7400, 77.2000),  # North Delhi
]

# API Keys from environment
TRAFFIC_API_KEY = os.getenv("TOMTOM_API_KEY")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

# Configuration
WEATHER_URL = f"http://api.openweathermap.org/data/2.5/weather?q=Delhi&appid={WEATHER_API_KEY}"
SLEEP_INTERVAL = 600  # 10 minutes (600 seconds)

def fetch_traffic_data():
    """Fetches traffic data from TomTom API for all locations and saves to a single JSON."""
    if not TRAFFIC_API_KEY:
        print("Error: TOMTOM_API_KEY not found in environment.")
        return

    all_traffic_data = []
    timestamp = datetime.now()

    print(f"[{timestamp}] Fetching traffic data for {len(LOCATIONS)} locations...")

    for lat, lon in LOCATIONS:
        try:
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={lat},{lon}&key={TRAFFIC_API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Add location metadata to the record
            data["latitude"] = lat
            data["longitude"] = lon
            data["ingestion_time"] = str(timestamp)
            
            all_traffic_data.append(data)
        except Exception as e:
            print(f"Error fetching traffic data for {lat}, {lon}: {e}")

    if all_traffic_data:
        file_name = f"traffic_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
        # We can nest the results or keep them as a list
        final_output = {
            "ingestion_time": str(timestamp),
            "data": all_traffic_data
        }

        with open(file_name, "w") as f:
            json.dump(final_output, f, indent=4)
        print(f"Successfully saved traffic data for {len(all_traffic_data)} locations to {file_name}")
    else:
        print("No traffic data was collected.")

def fetch_weather_data():
    """Fetches weather data from OpenWeatherMap API and saves to JSON."""
    if not WEATHER_API_KEY:
        print("Error: WEATHER_API_KEY not found in environment.")
        return

    try:
        print(f"[{datetime.now()}] Fetching weather data...")
        response = requests.get(WEATHER_URL)
        response.raise_for_status()
        data = response.json()
        
        data["ingestion_time"] = str(datetime.now())
        file_name = f"weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(file_name, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Successfully saved weather data: {file_name}")
    except Exception as e:
        print(f"Error fetching weather data: {e}")

def main():
    print("Starting Unified Ingestion System (Interval: 10 minutes)")
    while True:
        # Run both tasks
        fetch_traffic_data()
        fetch_weather_data()
        
        print(f"Sleeping for {SLEEP_INTERVAL} seconds...")
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    main()
