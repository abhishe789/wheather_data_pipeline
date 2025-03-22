import pandas as pd
import json

# Load your JSON data (assuming it's stored in a file or a variable)
with open("india_weather_data.json", "r", encoding="utf-8") as f:
    weather_data = json.load(f)  # Load JSON data

# Convert JSON to DataFrame
df = pd.DataFrame(weather_data)

# Save to CSV file
df.to_csv("weather_data.csv", index=False)

print("Weather data saved to weather_data.csv")
