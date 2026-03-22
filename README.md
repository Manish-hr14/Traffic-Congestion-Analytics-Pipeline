# Traffic and Weather Data Ingestion System

A modular and automated data ingestion system for real-time traffic and weather data, designed for Data Engineering pipelines.

## Features

- **Automated Ingestion**: Unified script that fetches data every 10 minutes.
- **Traffic Data**: Real-time traffic flow data from [TomTom Traffic API](https://developer.tomtom.com/).
- **Weather Data**: Current weather data from [OpenWeatherMap API](https://openweathermap.org/api).
- **Security**: API keys are managed via environment variables to prevent accidental exposure.
- **Robustness**: Independent error handling for each API service.

## Project Structure

```text
traffic-project/
├── ingestion/
│   ├── ingestion.py      # Unified automation script
│   ├── traffic_api.py    # Traffic lookup logic
│   └── weather_api.py    # Weather lookup logic
├── data/
│   └── raw/              # Locally stored JSON data
├── .env                  # Environment configuration (ignored by git)
├── .gitignore            # Git exclusion rules
├── README.md             # Project documentation
└── requirements.txt      # Python dependencies
```

## Setup Instructions

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd traffic-project
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure API Keys
Create a `.env` file in the root directory and add your API keys:
```text
TOMTOM_API_KEY=your_tomtom_key
WEATHER_API_KEY=your_weather_key
```

## Usage

Run the unified ingestion script to start the automation:
```bash
python ingestion/ingestion.py
```

Data will be saved as timestamped JSON files in the `ingestion/` directory (configured for local raw data storage).
