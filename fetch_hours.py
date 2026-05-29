#!/usr/bin/env python3
"""
fetch_hours.py  –  scrape opening-hours data from islandhealthproject.com
and write hours.json for consumption by index.html.

Output structure:
{
  "fetched_at": "2026-05-29T14:00:00Z",
  "businesses": [
    {
      "name": "Doctor's Office",
      "schedules": [
        {
          "label": "Off Peak",
          "start_date": "2025-09-06",   # YYYY-MM-DD, inclusive
          "end_date":   "2026-06-21",   # YYYY-MM-DD, inclusive
          "hours_by_dow": [             # index 0=Sun … 6=Sat
            "Closed", "5–7pm", "9am–12pm", "Closed",
            "9am–12pm", "9am–12pm", "Closed"
          ]
        },
        ...
      ]
    }
  ]
}

If scraping fails the file is still written using built-in fallback data so the
page always has something to show.
"""

import asyncio
import json
import re
from datetime import datetime, timezone
from html.parser import HTMLParser

from playwright.async_api import async_playwright

# ── Target ─────────────────────────────────────────────────────────────────
URL = "https://islandhealthproject.com/"

# ── Day-name lookup ─────────────────────────────────────────────────────────
_DAYS = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"]
_DAY_INDEX = {d: i for i, d in enumerate(_DAYS)}

# ── Fallback data (known-good as of May 2026) ───────────────────────────────
FALLBACK_SCHEDULES = [
    {
        "label": "Off Peak",
        "start_date": "2025-09-06",
        "end_date":   "2026-06-21",
        "hours_by_dow": [
            "Closed",      # Sun
            "5\u20137pm",  # Mon  (–)
            "9am\u201312pm",  # Tue
            "Closed",      # Wed
            "9am\u201312pm",  # Thu
            "9am\u201312pm",  # Fri
            "Closed",      # Sat
        ],
    },
    {
        "label": "Peak",
        "start_date": "2026-06-22",
        "end_date":   "2026-09-07",
        "hours_by_dow": [
            "Closed",                    # Sun
            "9am\u201312pm & 1\u20134pm",  # Mon
            "9am\u201312:30pm",            # Tue
            "9am\u201312pm & 1\u20134pm",  # Wed
            "9am\u201312:30pm",            # Thu
            "9am\u201312:30pm",            # Fri
            "9am\u201312:30pm",            # Sat
        ],
    },
]


# ── Minimal HTML parser (no external deps) ──────────────────────────────────
class _TableParser(HTMLParser):
    """Walk the HTML once and collect (heading-text, table-rows) pairs."""

    def __init__(self):
        super().__init__()
        self._stack = []          # current open tags
        self._cur_text = []       # text accumulator for current element
        self.sections = []        # list of {"heading": str, "rows": [[cell, ...]]}

        self._in_heading = False
        self._heading_text = ""
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._cur_row = []
        self._cur_cell_text = []
        self._cur_table_rows = []
        self._pending_heading = ""  # last heading seen before current table

    # helpers
    def _push(self, tag):  self._stack.append(tag)
    def _pop(self, tag):
        if self._stack and self._stack[-1] == tag:
            self._stack.pop()

    def handle_starttag(self, tag, attrs):
        self._push(tag)
        if tag in ("h2","h3","h4","h5"):
            self._in_heading = True
            self._heading_text = ""
        elif tag == "table":
            self._in_table = True
            self._cur_table_rows = []
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._cur_row = []
        elif tag in ("td","th") and self._in_row:
            self._in_cell = True
            self._cur_cell_text = []

    def handle_endtag(self, tag):
        self._pop(tag)
        if tag in ("h2","h3","h4","h5") and self._in_heading:
            self._in_heading = False
            self._pending_heading = self._heading_text.strip()
        elif tag == "table" and self._in_table:
            self._in_table = False
            self.sections.append({
                "heading": self._pending_heading,
                "rows": self._cur_table_rows,
            })
            self._cur_table_rows = []
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._cur_row:
                self._cur_table_rows.append(self._cur_row)
            self._cur_row = []
        elif tag in ("td","th") and self._in_cell:
            self._in_cell = False
            text = " ".join("".join(self._cur_cell_text).split())
            self._cur_row.append(text)
            self._cur_cell_text = []

    def handle_data(self, data):
        if self._in_heading:
            self._heading_text += data
        elif self._in_cell:
            self._cur_cell_text.append(data)


# ── Date-range helpers ──────────────────────────────────────────────────────
_MONTH_ABBR = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

def _parse_date(s: str):
    """Return 'YYYY-MM-DD' or None from strings like 'Sep 6, 2025'."""
    s = s.strip().rstrip(".")
    # Try standard formats via strptime
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Fallback regex: Month DD YYYY or Month DD, YYYY
    m = re.match(r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})', s)
    if m:
        mon = _MONTH_ABBR.get(m.group(1).lower())
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(2)):02d}"
    return None

def _parse_date_range(text: str):
    """Extract (start_date, end_date) as YYYY-MM-DD strings from a heading."""
    # Normalise various dash/en-dash/em-dash characters
    text = re.sub(r'[–—−]', '-', text)
    # Look for two date-like tokens separated by ' - '
    m = re.search(
        r'([A-Za-z]+\.?\s+\d{1,2},?\s+\d{4})\s*-\s*([A-Za-z]+\.?\s+\d{1,2},?\s+\d{4})',
        text
    )
    if m:
        return _parse_date(m.group(1)), _parse_date(m.group(2))
    return None, None


# ── Core parse logic ────────────────────────────────────────────────────────
def parse_hours_html(html: str):
    """Return a list of schedule dicts, or [] on failure."""
    parser = _TableParser()
    parser.feed(html)

    schedules = []
    for section in parser.sections:
        heading = section["heading"]
        rows = section["rows"]

        # Only process tables that have at least one row with a day name
        day_rows = {}
        for row in rows:
            if not row:
                continue
            day_key = row[0].lower().rstrip(":").strip()
            if day_key in _DAY_INDEX and len(row) >= 2:
                day_rows[_DAY_INDEX[day_key]] = row[1]

        if not day_rows:
            continue  # not a hours table

        hours_by_dow = [day_rows.get(i, "Closed") for i in range(7)]

        h_lower = heading.lower()
        if "peak" in h_lower and "off" not in h_lower and "non" not in h_lower:
            label = "Peak"
        elif "off" in h_lower or "non" in h_lower or "non-peak" in h_lower:
            label = "Off Peak"
        else:
            label = heading[:60]

        start_date, end_date = _parse_date_range(heading)

        # If date range not in heading, look a bit further — scan all headings
        # for a date range near the word "peak"
        if not start_date:
            for s in parser.sections:
                h = s["heading"]
                h_l = h.lower()
                if label.lower() in h_l or ("peak" in h_l and label == "Peak") \
                        or ("off" in h_l and label == "Off Peak"):
                    sd, ed = _parse_date_range(h)
                    if sd:
                        start_date, end_date = sd, ed
                        break

        schedules.append({
            "label": label,
            "start_date": start_date or "",
            "end_date":   end_date   or "",
            "hours_by_dow": hours_by_dow,
        })

    return schedules


# ── Playwright fetch ─────────────────────────────────────────────────────────
async def _fetch_page(url: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"  → GET {url}", flush=True)
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)
        html = await page.content()
        await browser.close()
        return html


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    used_fallback = False

    try:
        html = asyncio.run(_fetch_page(URL))
        schedules = parse_hours_html(html)

        if not schedules:
            print("  ⚠ No hour tables parsed — using fallback", flush=True)
            schedules = FALLBACK_SCHEDULES
            used_fallback = True
        else:
            print(f"  ✓ Parsed {len(schedules)} schedule(s) from live page", flush=True)
            for s in schedules:
                print(f"    • {s['label']}  {s['start_date']} – {s['end_date']}", flush=True)

    except Exception as exc:
        print(f"  ✗ Fetch/parse error: {exc} — using fallback", flush=True)
        schedules = FALLBACK_SCHEDULES
        used_fallback = True

    data = {
        "fetched_at": now_iso,
        "used_fallback": used_fallback,
        "businesses": [
            {
                "name": "Doctor's Office",
                "url": URL,
                "schedules": schedules,
            }
        ],
    }

    with open("hours.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("  ✓ hours.json written", flush=True)


if __name__ == "__main__":
    main()
