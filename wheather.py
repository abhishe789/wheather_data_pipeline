import requests
import json


API_KEY = "your key"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Load city list JSON (Download from OpenWeather and save as "city.list.json")
with open("/city.list.json", "r", encoding="utf-8") as f:
    cities = json.load(f)

# Filter only Indian cities (country code: IN)
indian_cities = [city for city in cities if city["country"] == "IN"]

# Fetch weather data for all Indian cities
weather_data = []
for city in indian_cities:  # Iterate over all cities instead of limiting to 10
    city_id = city["id"]
    city_name = city["name"]

    response = requests.get(f"{BASE_URL}?id={city_id}&appid={API_KEY}&units=metric")

    if response.status_code == 200:
        data = response.json()
        weather_info = {
            "city": city_name,
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["description"]
        }
        weather_data.append(weather_info)
    else:
        print(f"Error fetching data for {city_name}")

# Save data to a JSON file
with open("india_weather_data.json", "w", encoding="utf-8") as f:
    json.dump(weather_data, f, indent=2)

print("Weather data for all Indian cities saved to india_weather_data.json")
