"""
fetch_schedule.py
Fetches the Fishers Island Ferry vehicle schedule for today + the next 13 days
(14 days total) and writes schedule.json to the current directory.

Requires: playwright  (pip install playwright && playwright install chromium)
"""

import asyncio
import csv
import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://www.fiferry.com/auto-ferries/"
DIRECTIONS = [
    ("From New London",    "1104", "1105"),
    ("From Fishers Island","1105", "1104"),
]
OUTPUT_FILE = Path(__file__).parent / "schedule.json"
HISTORY_FILE = Path(__file__).parent / "ferry_history.csv"
HISTORY_FIELDS = ["fetched_at", "sailing_date", "direction", "time", "vehicle_spaces"]

# Regex patterns for extracting data from the Hornblower iframe text
TIME_RE    = re.compile(r'\b(\d{1,2}:\d{2}\s+(?:AM|PM))\b')
SPACES_RE  = re.compile(r'(\d+)\s+Total Vehicle Capacity Car Spaces Left')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def build_url(date_str: str, departure: str, destination: str) -> str:
    return f"{BASE_URL}?date={date_str}&departure={departure}&destination={destination}"


async def get_hornblower_frame(page):
    """Return the Hornblower booking iframe frame object, or None."""
    for frame in page.frames:
        url = frame.url or ""
        if "hornblower.com" in url:
            return frame
    # Fallback: find any non-main frame that mentions vehicle capacity
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        try:
            text = await frame.inner_text("body", timeout=3000)
            if "Vehicle Capacity" in text or ("AM" in text and "PM" in text):
                return frame
        except Exception:
            pass
    return None


async def fetch_sailings(page, date_str: str, departure: str, destination: str) -> list:
    url = build_url(date_str, departure, destination)
    print(f"  → {url}", flush=True)

    # Retry loop: cold-start on the first page load can leave the iframe
    # empty; subsequent attempts with longer waits almost always succeed.
    ATTEMPTS   = 4
    WAIT_MS    = [8000, 12000, 16000, 20000]   # progressive back-off per attempt

    for attempt in range(ATTEMPTS):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            # A single slow/failed page load must not crash the whole run.
            print(f"    ⚠ attempt {attempt + 1}: page load failed ({e})", flush=True)
            continue
        wait = WAIT_MS[attempt]
        print(f"    attempt {attempt + 1}: waiting {wait // 1000}s …", flush=True)
        await page.wait_for_timeout(wait)

        # Scroll to trigger lazy loading
        try:
            await page.mouse.wheel(0, 400)
            await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"    ⚠ Could not scroll page: {e}", flush=True)

        frame = await get_hornblower_frame(page)
        if frame is None:
            print("    ⚠ Hornblower frame not found", flush=True)
            continue

        try:
            text = await frame.inner_text("body", timeout=5000)
        except Exception as e:
            print(f"    ⚠ Could not read frame text: {e}", flush=True)
            continue

        times  = TIME_RE.findall(text)
        spaces = SPACES_RE.findall(text)

        if not times:
            print("    ⚠ No sailings parsed — retrying …", flush=True)
            continue

        sailings = []
        for i, t in enumerate(times):
            vehicle_spaces = int(spaces[i]) if i < len(spaces) else None
            sailings.append({"time": t.strip(), "vehicle_spaces": vehicle_spaces})

        print(f"    ✓ {len(sailings)} sailing(s) found", flush=True)
        return sailings

    print("    ✗ All attempts failed — returning empty list", flush=True)
    return []


def append_history(result: dict) -> int:
    """Append every sailing observation in `result` to the append-only
    ferry_history.csv log. Each row records the fetch timestamp alongside the
    sailing it describes, so the file accumulates a full history of how
    vehicle availability evolves for each date over time. Returns the number
    of rows written."""
    fetched_at = result.get("fetched_at", "")
    rows = []
    for day in result.get("days", []):
        for direction in day.get("directions", []):
            if direction.get("stale"):
                # Carried over from an earlier run — already logged then.
                continue
            for sailing in direction.get("sailings", []):
                rows.append({
                    "fetched_at": fetched_at,
                    "sailing_date": day.get("date", ""),
                    "direction": direction.get("direction", ""),
                    "time": sailing.get("time", ""),
                    "vehicle_spaces": sailing.get("vehicle_spaces"),
                })

    if not rows:
        return 0

    file_exists = HISTORY_FILE.exists()
    with HISTORY_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HISTORY_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def load_previous_sailings() -> dict:
    """Return {(iso_date, direction): (sailings, fetched_at)} from the existing
    schedule.json, so a failed fetch can fall back to the last good data
    instead of showing no sailings at all.  fetched_at is when that data was
    actually scraped (preserved across repeated fallbacks)."""
    try:
        prev = json.loads(OUTPUT_FILE.read_text())
    except Exception as e:
        print(f"⚠ No previous schedule to fall back on ({e})", flush=True)
        return {}
    prev_fetched_at = prev.get("fetched_at", "")
    sailings = {}
    for day in prev.get("days", []):
        for direction in day.get("directions", []):
            if direction.get("sailings"):
                sailings[(day.get("date"), direction.get("direction"))] = (
                    direction["sailings"],
                    direction.get("stale_from") or prev_fetched_at,
                )
    return sailings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    today = date.today()
    target_dates = [today + timedelta(days=i) for i in range(0, 14)]

    result = {
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "days": [],
    }
    prev_sailings = load_previous_sailings()
    missed, carried = [], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        for d in target_dates:
            date_str = d.strftime("%m/%d/%Y")
            iso_str  = d.strftime("%Y-%m-%d")
            label    = d.strftime("%A, %B %-d, %Y")

            print(f"\n[{label}]", flush=True)
            day_entry = {"date": iso_str, "label": label, "directions": []}

            for direction_name, departure, destination in DIRECTIONS:
                print(f"  {direction_name}", flush=True)
                sailings = await fetch_sailings(page, date_str, departure, destination)
                entry = {"direction": direction_name, "sailings": sailings}
                if not sailings:
                    missed.append(f"{iso_str} {direction_name}")
                    previous = prev_sailings.get((iso_str, direction_name))
                    if previous:
                        # Keep the last good data rather than blanking the day.
                        prev_list, prev_when = previous
                        print(f"    ↺ Keeping {len(prev_list)} sailing(s) from {prev_when}", flush=True)
                        entry = {
                            "direction": direction_name,
                            "sailings": prev_list,
                            "stale": True,
                            "stale_from": prev_when,
                        }
                        carried.append(f"{iso_str} {direction_name}")
                day_entry["directions"].append(entry)

            result["days"].append(day_entry)

        await browser.close()

    OUTPUT_FILE.write_text(json.dumps(result, indent=2))
    total = sum(
        len(d["sailings"])
        for day in result["days"]
        for d in day["directions"]
    )
    print(f"\n✓ Wrote {OUTPUT_FILE} — {total} total sailings", flush=True)

    n_hist = append_history(result)
    print(f"✓ Appended {n_hist} observation(s) to {HISTORY_FILE.name}", flush=True)

    if missed:
        print(f"\n⚠ {len(missed)} fetch(es) returned no sailings:", flush=True)
        for m in missed:
            note = " (kept previous data)" if m in carried else " (no previous data)"
            print(f"    {m}{note}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
