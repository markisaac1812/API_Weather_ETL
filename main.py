import requests
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import time

# load env variables
load_dotenv(".env", override=True)

url = "https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}"
api_key = os.getenv("API_KEY")

# Setup DB connection (do this ONCE, before the loop)
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")

db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=username,
    password=password,
    host=host,
    port=int(port) if port else None,
    database=database,
)
engine = create_engine(db_url)

# Add unique constraint to existing table (run once, won't fail if already exists)
try:
    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE city_weather 
            ADD CONSTRAINT unique_city_timestamp UNIQUE (city, timestamp)
        """))
        conn.commit()
        print("Added unique constraint to table")
except Exception as e:
    # Constraint might already exist, that's fine
    print(f"Note: {e}")

cities = [
    "Cairo", "Alexandria", "Giza",  # Egypt
    "London", "Paris", "Berlin", "Madrid", "Rome",  # Europe
    "New York", "Los Angeles", "Chicago", "Toronto",  # North America
    "Tokyo", "Seoul", "Beijing", "Shanghai", "Mumbai",  # Asia
    "Sydney", "Melbourne",  # Australia
    "São Paulo", "Rio de Janeiro",  # South America
]

for city in cities:
    try:
        # Extract
        new_url = url.replace("{city name}", city).replace("{API key}", api_key)
        response = requests.get(new_url)
        data = response.json()
        
        if response.status_code != 200:
            print(f"Error fetching {city}: {response.status_code} - {data.get('message')}")
            continue  # Skip this city, move to next
        
        print(f"✓ {city} weather fetched successfully")

        # 2) Transform
        transformed = {
            "city": data["name"],
            "country": data["sys"]["country"],
            "temp_celsius": round(data["main"]["temp"] - 273.15, 2),
            "feels_like_celsius": round(data["main"]["feels_like"] - 273.15, 2),
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"]["speed"],
            "description": data["weather"][0]["description"],
            "timestamp": pd.to_datetime(data["dt"], unit="s"),
        }

        # 3) Load with UPSERT logic
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO city_weather 
                (city, country, temp_celsius, feels_like_celsius, humidity, pressure, wind_speed, description, timestamp)
                VALUES (:city, :country, :temp_celsius, :feels_like_celsius, :humidity, :pressure, :wind_speed, :description, :timestamp)
                ON CONFLICT (city, timestamp) 
                DO UPDATE SET 
                    country = EXCLUDED.country,
                    temp_celsius = EXCLUDED.temp_celsius,
                    feels_like_celsius = EXCLUDED.feels_like_celsius,
                    humidity = EXCLUDED.humidity,
                    pressure = EXCLUDED.pressure,
                    wind_speed = EXCLUDED.wind_speed,
                    description = EXCLUDED.description
            """), transformed)
            conn.commit()
        
        print(f"✓ {city} data loaded to Postgres successfully")
        
        # Rate limiting to avoid hitting API limits
        time.sleep(0.5)
        
    except Exception as e:
        print(f"✗ Error processing {city}: {e}")
        continue  # Continue with next city even if this one fails

print("\n🎉 ETL pipeline completed!")