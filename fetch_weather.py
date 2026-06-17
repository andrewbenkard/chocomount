#!/usr/bin/env python3
"""
fetch_weather.py — fetch a 14-day weather forecast for Fishers Island, NY
from the Open-Meteo API (free, no API key required) and write
weather.json for consumption by index.html.

Output structure:
{
  "fetched_at": "2026-05-29T14:00:00Z",
  "days": {
    "2026-05-29": "Mostly Sunny, 68°F",
    "2026-05-30": "Partly Cloudy, 72°F",
    ...
  }
}
"""

import json
import sys
import traceback
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# Fishers Island, NY  (41.26 °N, 71.96 °W)
_LAT = 41.2626
_LON = -71.9592

# Open-Meteo — free, no API key, works for any coordinates including islands
_BASE = "https://api.open-meteo.com/v1/forecast"
_PARAMS = urllib.parse.urlencode({
    "latitude":         _LAT,
    "longitude":        _LON,
    "daily":            "weather_code,temperature_2m_max",
    "temperature_unit": "fahrenheit",
    "timezone":         "America/New_York",
    "forecast_days":    14,
})
_URL = f"{_BASE}?{_PARAMS}"

# WMO weather interpretation codes → short description
_WMO = {
    0:  "Clear sky",
    1:  "Mainly clear",
    2:  "Partly cloudy",
    3:  "Overcast",
    45: "Foggy",
    48: "Icy fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Heavy drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    71: "Light snow",
    73: "Snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Light showers",
    81: "Showers",
    82: "Heavy showers",
    85: "Snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm w/ hail",
    99: "Thunderstorm w/ hail",
}


def fetch_forecast() -> dict:
    """Return weather.json-ready dict with 'fetched_at' and 'days'."""
    print(f"  → GET {_URL}", flush=True)
    req = urllib.request.Request(_URL, headers={"User-Agent": "chocomount/1.0 (andrew@benkard.com)"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())

    daily   = data["daily"]
    dates   = daily["time"]               # ["YYYY-MM-DD", ...]
    codes   = daily["weather_code"]       # [0, 1, 2, ...]
    temps   = daily["temperature_2m_max"] # [72.5, 68.1, ...]

    days: dict[str, str] = {}
    for date_str, code, temp in zip(dates, codes, temps):
        description = _WMO.get(int(code), f"Code {code}")
        temp_f      = round(temp)
        days[date_str] = f"{description}, {temp_f}°F"

    return {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
    }


def main():
    print("Fetching weather forecast (Open-Meteo) …", flush=True)
    try:
        data = fetch_forecast()
        print(f"  ✓ {len(data['days'])} day(s) of forecast written", flush=True)
    except Exception as e:
        print(f"  ⚠ Weather fetch failed: {e}", flush=True)
        traceback.print_exc()
        data = {
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "days": {},
        }
        with open("weather.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print("✓ weather.json written (empty fallback)", flush=True)
        sys.exit(1)

    with open("weather.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("✓ weather.json written", flush=True)


if __name__ == "__main__":
    main()
