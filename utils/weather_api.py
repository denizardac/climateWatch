import requests
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherDataFetcher:
    def __init__(self):
        """Initialize weather data fetcher for Open-Meteo API"""
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        
        # Turkish cities coordinates
        self.cities = {
            "Istanbul": {"lat": 41.0082, "lon": 28.9784},
            "Ankara": {"lat": 39.9334, "lon": 32.8597},
            "Izmir": {"lat": 38.4237, "lon": 27.1428},
            "Antalya": {"lat": 36.8969, "lon": 30.7133}
        }

    def get_weather_data(self, city: str) -> Dict[str, Any]:
        """
        Get current weather data for a city
        
        Args:
            city: City name (must be one of the predefined cities)
            
        Returns:
            Dictionary containing weather data
        """
        if city not in self.cities:
            raise ValueError(f"City {city} not supported. Must be one of {list(self.cities.keys())}")
        
        coords = self.cities[city]
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,surface_pressure",
            "hourly": "temperature_2m,precipitation,snow_depth",
            "timezone": "auto"
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            current = data.get('current', {})
            
            # Get current hour's snow depth from hourly data
            current_hour = datetime.now().hour
            snow_depth = data.get('hourly', {}).get('snow_depth', [])[current_hour] if data.get('hourly', {}).get('snow_depth') else 0
            
            return {
                "timestamp": datetime.now().isoformat(),
                "location": city,
                "temperature": round(float(current.get('temperature_2m', 0)), 2),
                "humidity": float(current.get('relative_humidity_2m', 0)),
                "wind_speed": float(current.get('wind_speed_10m', 0)),
                "precipitation": float(current.get('precipitation', 0)),
                "pressure": float(current.get('surface_pressure', 0)),
                "snow": float(snow_depth)
            }
        except Exception as e:
            logger.error(f"Error fetching weather data for {city}: {str(e)}")
            raise 