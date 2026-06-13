#!/usr/bin/env python3
"""
fetch_tides.py — fetch a 10-day high/low tide forecast for Fishers Island, NY
from the NOAA Tides & Currents API (free, no API key required) and write
tides.json for consumption by index.html.

Station: Silver Eel Pond, Fishers Island, NY (NOAA 8510719) — the ferry harbor.

Output structure:
{
  "fetched_at": "2026-06-13T18:00:00Z",
  "station": "Silver Eel Pond, Fishers Island, NY",
  "station_id": "8510719",
  "datum": "MLLW",
  "days": {
    "2026-06-13": [
      {"type": "L", "time": "2:44 AM", "height": -0.1},
      {"type": "H", "time": "8:19 AM", "height": 2.3},
      ...
    ],
    ...
  }
}
"""

import json
import sys
import traceback
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _EASTERN = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - zoneinfo always present on 3.9+
    _EASTERN = timezone.utc

# Silver Eel Pond, Fishers Island, NY — the ferry harbor.
_STATION_ID = "8510719"
_STATION_NAME = "Silver Eel Pond, Fishers Island, NY"
_DATUM = "MLLW"
_FORECAST_DAYS = 10

_BASE = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"


def _build_url(begin: str, end: str) -> str:
    params = urllib.parse.urlencode({
        "product":     "predictions",
        "application": "chocomount",
        "datum":       _DATUM,
        "station":     _STATION_ID,
        "time_zone":   "lst_ldt",      # local standard / daylight time at the station
        "units":       "english",      # feet
        "interval":    "hilo",         # high/low only
        "begin_date":  begin,
        "end_date":    end,
        "format":      "json",
    })
    return f"{_BASE}?{params}"


def _fmt_time(hhmm: str) -> str:
    """'08:19' -> '8:19 AM', '20:42' -> '8:42 PM'."""
    h, m = hhmm.split(":")
    h = int(h)
    suffix = "AM" if h < 12 else "PM"
    h12 = h % 12 or 12
    return f"{h12}:{m} {suffix}"


def fetch_tides() -> dict:
    """Return tides.json-ready dict with 'fetched_at', station info, and 'days'."""
    today = datetime.now(_EASTERN).date()
    begin = today.strftime("%Y%m%d")
    end = (today + timedelta(days=_FORECAST_DAYS - 1)).strftime("%Y%m%d")
    url = _build_url(begin, end)

    print(f"  → GET {url}", flush=True)
    req = urllib.request.Request(
        url, headers={"User-Agent": "chocomount/1.0 (andrew@benkard.com)"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())

    predictions = data.get("predictions", [])
    if not predictions:
        # NOAA returns {"error": {...}} when something is wrong.
        if "error" in data:
            raise RuntimeError(data["error"].get("message", "NOAA error"))
        raise RuntimeError("No predictions returned")

    days: dict[str, list] = {}
    for p in predictions:
        # p["t"] == "YYYY-MM-DD HH:MM"
        date_str, time_str = p["t"].split(" ")
        try:
            height = round(float(p["v"]), 1)
        except (KeyError, ValueError):
            height = None
        days.setdefault(date_str, []).append({
            "type":   p.get("type", ""),   # "H" or "L"
            "time":   _fmt_time(time_str),
            "height": height,
        })

    return {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "station":    _STATION_NAME,
        "station_id": _STATION_ID,
        "datum":      _DATUM,
        "days":       days,
    }


def main():
    print("Fetching tide forecast (NOAA Tides & Currents) …", flush=True)
    try:
        data = fetch_tides()
        print(f"  ✓ {len(data['days'])} day(s) of tides written", flush=True)
    except Exception as e:
        print(f"  ⚠ Tide fetch failed: {e}", flush=True)
        traceback.print_exc()
        data = {
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "station":    _STATION_NAME,
            "station_id": _STATION_ID,
            "datum":      _DATUM,
            "days":       {},
        }
        with open("tides.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print("✓ tides.json written (empty fallback)", flush=True)
        sys.exit(1)

    with open("tides.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("✓ tides.json written", flush=True)


if __name__ == "__main__":
    main()
