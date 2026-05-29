#!/usr/bin/env python3
"""
fetch_weather.py — fetch a 7-day weather forecast for Fishers Island, NY
from the National Weather Service API (no API key required) and write
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
import urllib.request
from datetime import datetime, timezone

# Fishers Island, NY  (41.26 °N, 71.96 °W)
_LAT = 41.2626
_LON = -71.9592
_NWS_POINTS = f"https://api.weather.gov/points/{_LAT},{_LON}"

# NWS requires a descriptive User-Agent header
_HEADERS = {"User-Agent": "chocomount/1.0 (andrew@benkard.com)"}


def _get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_forecast() -> dict:
    """Return weather.json-ready dict with 'fetched_at' and 'days'."""
    # Step 1: resolve the grid-point for our coordinates
    print(f"  → GET {_NWS_POINTS}", flush=True)
    points = _get_json(_NWS_POINTS)
    forecast_url = points["properties"]["forecast"]

    # Step 2: fetch the 7-day / 14-period forecast
    print(f"  → GET {forecast_url}", flush=True)
    forecast = _get_json(forecast_url)

    days: dict[str, str] = {}
    for period in forecast["properties"]["periods"]:
        if not period.get("isDaytime", True):
            continue  # only use the daytime period for each date

        date_str = period["startTime"][:10]   # "YYYY-MM-DD"
        temp     = period["temperature"]
        unit     = period["temperatureUnit"]   # "F"
        short    = period["shortForecast"]

        days[date_str] = f"{short}, {temp}°{unit}"

    return {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
    }


def main():
    print("Fetching weather forecast (NWS) …", flush=True)
    try:
        data = fetch_forecast()
        print(f"  ✓ {len(data['days'])} day(s) of forecast written", flush=True)
    except Exception as e:
        print(f"  ⚠ Weather fetch failed ({e}) — writing empty fallback", flush=True)
        data = {
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "days": {},
        }

    with open("weather.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("✓ weather.json written", flush=True)


if __name__ == "__main__":
    main()
