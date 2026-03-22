import requests
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Keys from environment
TRAFFIC_API_KEY = os.getenv("TOMTOM_API_KEY")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

# Configuration
TRAFFIC_URL = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point=28.7041,77.1025&key={TRAFFIC_API_KEY}"
WEATHER_URL = f"http://api.openweathermap.org/data/2.5/weather?q=Delhi&appid={WEATHER_API_KEY}"
SLEEP_INTERVAL = 600  # 10 minutes (600 seconds)

def fetch_traffic_data():
    """Fetches traffic data from TomTom API and saves to JSON."""
    if not TRAFFIC_API_KEY:
        print("Error: TOMTOM_API_KEY not found in environment.")
        return

    try:
        print(f"[{datetime.now()}] Fetching traffic data...")
        response = requests.get(TRAFFIC_URL)
        response.raise_for_status()
        data = response.json()
        
        data["ingestion_time"] = str(datetime.now())
        file_name = f"traffic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(file_name, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Successfully saved traffic data: {file_name}")
    except Exception as e:
        print(f"Error fetching traffic data: {e}")

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
