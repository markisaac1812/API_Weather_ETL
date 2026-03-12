from airflow.decorators import dag, task
from datetime import datetime
import requests
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import time


@dag(
    description='Weather ETL Pipeline',
    tags=['data_engineering_team'],
    schedule='@daily',
    start_date=datetime(2026, 3, 13),
    catchup=False
)
def weather_etl_pipeline():
    
    @task
    def extract_weather_data():
        """Extract weather data from OpenWeather API for multiple cities"""
        
        # Load configuration
        api_key = os.getenv("API_KEY")
        url = "https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}"
        
        cities = [
            "Cairo", "Alexandria", "Giza",
            "London", "Paris", "Berlin", "Madrid", "Rome",
            "New York", "Los Angeles", "Chicago", "Toronto",
            "Tokyo", "Seoul", "Beijing", "Shanghai", "Mumbai",
            "Sydney", "Melbourne",
            "São Paulo", "Rio de Janeiro",
        ]
        
        all_weather_data = []
        
        for city in cities:
            try:
                new_url = url.replace("{city name}", city).replace("{API key}", api_key)
                response = requests.get(new_url)
                data = response.json()
                
                if response.status_code != 200:
                    print(f"Error fetching {city}: {response.status_code} - {data.get('message')}")
                    continue
                
                print(f"✓ {city} weather fetched successfully")
                all_weather_data.append(data)
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"✗ Error fetching {city}: {e}")
                continue
        
        return all_weather_data
    
    
    @task
    def transform_weather_data(raw_data: list):
        """Transform raw weather data into clean format"""
        
        transformed_data = []
        
        for data in raw_data:
            try:
                transformed = {
                    "city": data["name"],
                    "country": data["sys"]["country"],
                    "temp_celsius": round(data["main"]["temp"] - 273.15, 2),
                    "feels_like_celsius": round(data["main"]["feels_like"] - 273.15, 2),
                    "humidity": data["main"]["humidity"],
                    "pressure": data["main"]["pressure"],
                    "wind_speed": data["wind"]["speed"],
                    "description": data["weather"][0]["description"],
                    "timestamp": pd.to_datetime(data["dt"], unit="s").isoformat(),  # Convert to string for XCom
                }
                transformed_data.append(transformed)
                print(f"✓ Transformed data for {transformed['city']}")
                
            except Exception as e:
                print(f"✗ Error transforming data: {e}")
                continue
        
        return transformed_data
    
    
    @task
    def load_to_postgres(transformed_data: list):
        """Load transformed data into PostgreSQL with upsert logic"""
        
        # Setup DB connection
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
        
        # Ensure unique constraint exists
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    ALTER TABLE city_weather 
                    ADD CONSTRAINT unique_city_timestamp UNIQUE (city, timestamp)
                """))
                conn.commit()
                print("Added unique constraint to table")
        except Exception as e:
            print(f"Constraint note: {e}")
        
        # Load data with upsert
        success_count = 0
        for record in transformed_data:
            try:
                # Convert timestamp string back to datetime
                record['timestamp'] = pd.to_datetime(record['timestamp'])
                
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
                    """), record)
                    conn.commit()
                
                print(f"✓ Loaded {record['city']} to Postgres")
                success_count += 1
                
            except Exception as e:
                print(f"✗ Error loading {record.get('city', 'unknown')}: {e}")
                continue
        
        print(f"\n🎉 ETL pipeline completed! Loaded {success_count}/{len(transformed_data)} records")
        return success_count
    
    
    # Define task dependencies
    raw_data = extract_weather_data()
    transformed_data = transform_weather_data(raw_data)
    load_to_postgres(transformed_data)


# Instantiate the DAG
weather_etl_pipeline()