# importing required modules
import requests
import pandas as pd
import json

# api url & api_key.
base_url = 'https://api.openweathermap.org/data/2.5/weather?q='
api_key = '&appid=54e6ad025ebbf135*****a4a6822667'

# I have taken some specifice city name:
world_city_name = ['Bangalore', 'San Francisco', 'Seattle', 'London', 'Austin', 'Dublin', 'Berlin', 'Toronto', 'New York City',
                   'Sydney', 'Tokyo', 'Singapore', 'Stockholm', 'Helsinki', 'Munich', 'Tel Aviv', 'Vancouver', 'Zurich', 'Shanghai', 'Barcelona']

# ingest data through api

def ingest_data():
    prod_data = []
    for city in world_city_name:
        weather_api = base_url + city + api_key
        ingested_data = requests.get(weather_api).json()
        
        # required data
        raw_data = {
        'country': ingested_data['sys']['country'],
        'city_name': ingested_data['name'],
        'temperature': ingested_data['main']['temp'],
        'sunrise': ingested_data['sys']['sunrise'],
        'sunset': ingested_data['sys']['sunset'],
        'timezone': ingested_data['timezone']}

        prod_data.append(raw_data)
    
    raw_data_frame = pd.DataFrame(prod_data)
    
    # load raw data in s3:
    raw_data_frame.to_csv("s3://datapipeline-raw/weather_raw_data.csv")
    return raw_data_frame

# run manually if need to check:
ingest_data()
