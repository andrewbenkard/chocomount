#!/usr/bin/env python3
"""
analyze_history.py — summarise ferry_history.csv into sailing_stats.json so
index.html can flag sailings that often sell out.

For every sailing that has already departed, the last observation before
departure is taken as its final car-space count.  Sailings are then grouped
by day of week + direction + departure time over a rolling window, and a
group is flagged "often_sells_out" when it was sold out (0 spaces) on at
least SOLD_OUT_THRESHOLD of at least MIN_SAILINGS sailings.

Grouping by day of week matters: e.g. the 8:15 AM from Fishers Island sells
out most Tuesdays but rarely on Saturdays.  The rolling window keeps the
flags in step with seasonal timetable changes.

Output structure:
{
  "generated_at": "2026-10-07T20:00:00Z",
  "window_days": 70,
  "min_sailings": 6,
  "sold_out_threshold": 0.333,
  "sailings": [
    {"dow": 2, "day": "Tuesday", "direction": "From Fishers Island",
     "time": "8:15 AM", "sailings": 10, "sold_out": 8,
     "sold_out_rate": 0.8, "often_sells_out": true},
    ...
  ]
}
"dow" uses JavaScript getDay() numbering: 0=Sunday … 6=Saturday.
"""

import csv
import json
import sys
import traceback
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    _EASTERN = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - zoneinfo always present on 3.9+
    _EASTERN = timezone.utc

HISTORY_FILE = Path(__file__).parent / "ferry_history.csv"
OUTPUT_FILE  = Path(__file__).parent / "sailing_stats.json"

WINDOW_DAYS        = 70     # look back ten weeks (~10 sailings per weekday)
MIN_SAILINGS       = 6      # need at least this many departed sailings
SOLD_OUT_THRESHOLD = 1 / 3  # sold out on ≥ a third of them → "often sells out"

_DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday",
              "Thursday", "Friday", "Saturday"]


def _js_dow(iso_date: str) -> int:
    """'2026-10-06' → 2 (JavaScript getDay(): 0=Sunday … 6=Saturday)."""
    return (date.fromisoformat(iso_date).weekday() + 1) % 7


def analyze() -> dict:
    today  = datetime.now(_EASTERN).date()
    cutoff = (today - timedelta(days=WINDOW_DAYS)).isoformat()
    today_s = today.isoformat()

    # Final observation per departed sailing: (date, direction, time) → (fetched_at, spaces)
    final: dict[tuple, tuple] = {}
    with open(HISTORY_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sdate = row["sailing_date"]
            if not (cutoff <= sdate < today_s):
                continue
            try:
                spaces = int(row["vehicle_spaces"])
            except (TypeError, ValueError):
                continue
            key = (sdate, row["direction"], row["time"])
            if key not in final or row["fetched_at"] > final[key][0]:
                final[key] = (row["fetched_at"], spaces)

    groups: dict[tuple, list] = defaultdict(list)
    for (sdate, direction, time), (_, spaces) in final.items():
        groups[(_js_dow(sdate), direction, time)].append(spaces)

    sailings = []
    for (dow, direction, time), counts in sorted(groups.items()):
        n = len(counts)
        sold_out = sum(1 for c in counts if c == 0)
        rate = sold_out / n
        sailings.append({
            "dow":             dow,
            "day":             _DAY_NAMES[dow],
            "direction":       direction,
            "time":            time,
            "sailings":        n,
            "sold_out":        sold_out,
            "sold_out_rate":   round(rate, 3),
            "often_sells_out": n >= MIN_SAILINGS and rate >= SOLD_OUT_THRESHOLD,
        })

    return {
        "generated_at":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days":        WINDOW_DAYS,
        "min_sailings":       MIN_SAILINGS,
        "sold_out_threshold": round(SOLD_OUT_THRESHOLD, 3),
        "sailings":           sailings,
    }


def main():
    print("Analyzing ferry history …", flush=True)
    try:
        data = analyze()
    except Exception as e:
        # Leave the previous sailing_stats.json in place rather than blanking it.
        print(f"  ⚠ History analysis failed: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)

    flagged = [f"{s['day'][:3]} {s['direction']} {s['time']}"
               for s in data["sailings"] if s["often_sells_out"]]
    print(f"  ✓ {len(data['sailings'])} sailing(s) analysed; "
          f"often sells out: {', '.join(flagged) or 'none'}", flush=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ {OUTPUT_FILE.name} written", flush=True)


if __name__ == "__main__":
    main()
