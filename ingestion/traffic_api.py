import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("TOMTOM_API_KEY")

if not API_KEY:
    print("Error: TOMTOM_API_KEY not found in environment.")
    exit(1)

url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point=28.7041,77.1025&key={API_KEY}"

response = requests.get(url)
data = response.json()

# Add timestamp
data["ingestion_time"] = str(datetime.now())

# Save to file
file_name = f"traffic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

with open(file_name, "w") as f:
    json.dump(data, f, indent=4)

print("Data saved:", file_name)