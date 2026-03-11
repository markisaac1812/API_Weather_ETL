import requests
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# load env variables
load_dotenv("e.env", override=True)

url = "https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}"
api_key = os.getenv("API_KEY")

# 1) extract 
cities = [
    "Cairo", "Alexandria", "Giza",  # Egypt
    "London", "Paris", "Berlin", "Madrid", "Rome",  # Europe
    "New York", "Los Angeles", "Chicago", "Toronto",  # North America
    "Tokyo", "Seoul", "Beijing", "Shanghai", "Mumbai",  # Asia
    "Sydney", "Melbourne",  # Australia
    "São Paulo", "Rio de Janeiro",  # South America
]
for city in cities:
    new_url = url.replace("{city name}", city).replace("{API key}", api_key)
    response = requests.get(new_url)
    data = response.json()
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {data.get('message')}")
        raise SystemExit(1)
    else:
        print(f"{city} city weather have been fetched succfully")    

    # 2) transform
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

    df_clean = pd.DataFrame([transformed])

    # 3) load into postgres DB
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
    try:
        df_clean.to_sql("city_weather", con=engine, if_exists='append', index=False)
        print(f"Loaded {city} data to Postgres successfully")
    except Exception as e:
        print("Error when loading to postgres:", e)