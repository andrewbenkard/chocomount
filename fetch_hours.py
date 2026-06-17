#!/usr/bin/env python3
"""
fetch_hours.py  –  scrape opening-hours data for Fishers Island businesses
and write hours.json for consumption by index.html.

Sources:
  • Doctor's Office  → https://islandhealthproject.com/
  • Transfer Station → https://fiwmd.net/
  • Compost Station  → https://fiwmd.net/
  • Library          → https://filibrary.org/
  • Village Market   → https://fishersisland.net/listing/village-market/
  • West End Cafe    → https://www.westendfi.com/  (hardcoded – site structure unknown)
  • Beach Plum       → https://fishersisland.net/listing/beach-plum/  (hardcoded – hours confirmed by user)

Output structure:
{
  "fetched_at": "2026-05-29T14:00:00Z",
  "businesses": [
    {
      "name": "Doctor's Office",
      "url": "https://islandhealthproject.com/",
      "schedules": [
        {
          "label": "Off Peak",
          "start_date": "2025-09-06",   # YYYY-MM-DD inclusive
          "end_date":   "2026-06-21",
          "hours_by_dow": [             # 0=Sun … 6=Sat
            "Closed", "5–7pm", ...
          ]
          # optional:
          # "holiday_closings": [{"date": "2026-01-01", "name": "New Year's Day"}, ...]
        }
      ]
    },
    ...
  ]
}
"""

import asyncio
import json
import re
from datetime import date, datetime, timezone
from html.parser import HTMLParser

from playwright.async_api import async_playwright

# ════════════════════════════════════════════════════════════════════════════
# Time-string normalisation
# ════════════════════════════════════════════════════════════════════════════

def _norm_token(s: str) -> str:
    """Normalise one time token: '9am' → '9:00 am', '12:30PM' → '12:30 pm'."""
    s = s.strip()
    s = re.sub(r'(\d)(am|pm)', r'\1 \2', s, flags=re.IGNORECASE)
    s = re.sub(r'(am|pm)', lambda m: m.group().lower(), s, flags=re.IGNORECASE)
    s = re.sub(r'^(\d{1,2})\s+(am|pm)$', r'\1:00 \2', s)
    return s


def _norm_range(s: str) -> str:
    """Normalise one time range: '9am-12pm' → '9:00 am – 12:00 pm'."""
    s = s.strip()
    m = re.match(
        r'^([\d:]+\s*(?:am|pm)?)\s*[-\u2013\u2014]\s*([\d:]+\s*(?:am|pm)?)$',
        s, re.IGNORECASE,
    )
    if not m:
        return s
    start, end = m.group(1).strip(), m.group(2).strip()
    end_ap   = re.search(r'(am|pm)', end,   re.IGNORECASE)
    start_ap = re.search(r'(am|pm)', start, re.IGNORECASE)
    if end_ap and not start_ap:
        start = start + end_ap.group(1).lower()
    return _norm_token(start) + ' \u2013 ' + _norm_token(end)


def normalize_hours(s: str) -> str:
    """
    Normalise any hours string to standard display format:
      • am/pm lowercase    • spaces around en-dash
      • :00 for bare hours • sentence-case "Closed"
      • fix merged ranges  e.g. '9am-12pm1pm-4pm' → '9:00 am – 12:00 pm & 1:00 pm – 4:00 pm'

    Examples:
      '9am-12pm'     → '9:00 am – 12:00 pm'
      '1-5pm'        → '1:00 pm – 5:00 pm'
      '7:30 AM–5 PM' → '7:30 am – 5:00 pm'
      'CLOSED'       → 'Closed'
    """
    # Collapse thin-space / narrow-no-break-space / non-breaking-space to regular space
    s = re.sub(r'[\u2009\u202f\u00a0]', ' ', s)
    if re.match(r'^\s*closed\s*$', s, re.IGNORECASE):
        return "Closed"
    s = re.sub(r'(am|pm)(\d)', r'\1 & \2', s, flags=re.IGNORECASE)
    parts = [p.strip() for p in s.split('&')]
    return ' & '.join(_norm_range(p) for p in parts)


# ════════════════════════════════════════════════════════════════════════════
# Fallback data
# ════════════════════════════════════════════════════════════════════════════

# ── Beach Plum ────────────────────────────────────────────────────────────────
BEACH_PLUM_URL = "https://fishersisland.net/listing/beach-plum/"

BEACH_PLUM_BUSINESS = {
    "name": "Beach Plum",
    "url":  BEACH_PLUM_URL,
    "schedules": [
        {
            "label": "Regular",
            "start_date": "",
            "end_date": "",
            # 0=Sun … 6=Sat
            "hours_by_dow": [
                "Closed",                                              # Sun
                "10:00 am \u2013 1:00 pm & 3:00 \u2013 5:00 pm",    # Mon
                "10:00 am \u2013 1:00 pm & 3:00 \u2013 5:00 pm",    # Tue
                "10:00 am \u2013 1:00 pm & 3:00 \u2013 5:00 pm",    # Wed
                "10:00 am \u2013 1:00 pm & 3:00 \u2013 5:00 pm",    # Thu
                "10:00 am \u2013 1:00 pm & 3:00 \u2013 5:00 pm",    # Fri
                "10:00 am \u2013 1:00 pm & 3:00 \u2013 5:00 pm",    # Sat
            ],
            "holiday_closings": [],
        }
    ],
}

# ── West End Cafe ─────────────────────────────────────────────────────────────
WEST_END_URL = "https://www.westendfi.com/"

WEST_END_BUSINESS = {
    "name": "West End Cafe",
    "url":  WEST_END_URL,
    "schedules": [
        {
            "label": "Regular",
            "start_date": "",
            "end_date": "",
            # 0=Sun … 6=Sat
            "hours_by_dow": [
                "7:00 am \u2013 4:00 pm",  # Sun
                "7:00 am \u2013 4:00 pm",  # Mon
                "7:00 am \u2013 4:00 pm",  # Tue
                "7:00 am \u2013 4:00 pm",  # Wed
                "7:00 am \u2013 4:00 pm",  # Thu
                "7:00 am \u2013 4:00 pm",  # Fri
                "7:00 am \u2013 4:00 pm",  # Sat
            ],
            "holiday_closings": [],
        }
    ],
}

# ── Fishers Island Freight Office ─────────────────────────────────────────────
FREIGHT_OFFICE_URL = "https://www.fiferry.com/"

FREIGHT_OFFICE_BUSINESS = {
    "name": "Fishers Island Freight Office",
    "url":  FREIGHT_OFFICE_URL,
    "schedules": [
        {
            "label": "Regular",
            "start_date": "",
            "end_date": "",
            # 0=Sun … 6=Sat
            "hours_by_dow": [
                "Closed",                       # Sun
                "8:00 am – 4:45 pm",       # Mon
                "8:00 am – 4:45 pm",       # Tue
                "8:00 am – 4:45 pm",       # Wed
                "8:00 am – 4:45 pm",       # Thu
                "8:00 am – 4:45 pm",       # Fri
                "7:30 am – 11:30 am",      # Sat
            ],
            "holiday_closings": [],
        }
    ],
}

# ── Doctor's Office (Island Health Project) ──────────────────────────────────
IHP_URL = "https://islandhealthproject.com/"

IHP_FALLBACK_SCHEDULES = [
    {
        "label": "Off Peak",
        "start_date": "2025-09-06",
        "end_date":   "2026-06-21",
        # 0=Sun … 6=Sat
        "hours_by_dow": [
            "Closed",       # Sun
            "5\u20137pm",   # Mon
            "9am\u201312pm",# Tue
            "Closed",       # Wed
            "9am\u201312pm",# Thu
            "9am\u201312pm",# Fri
            "Closed",       # Sat
        ],
    },
    {
        "label": "Peak",
        "start_date": "2026-06-22",
        "end_date":   "2026-09-07",
        "hours_by_dow": [
            "Closed",                       # Sun
            "9am\u201312pm & 1\u20134pm",   # Mon
            "9am\u201312:30pm",             # Tue
            "9am\u201312pm & 1\u20134pm",   # Wed
            "9am\u201312:30pm",             # Thu
            "9am\u201312:30pm",             # Fri
            "9am\u201312:30pm",             # Sat
        ],
    },
]

# ── Waste Management (Transfer Station + Compost Station) ────────────────────
# Source: fishersisland.net/listing/waste-management/ (2025 schedule → 2026)
# Python weekday() → JS getDay() mapping:  Mon=0→1  Tue=1→2  …  Sun=6→0
WMD_URL = "https://fiwmd.net/"

WMD_HOLIDAY_FALLBACK = [
    {"date": "2026-01-01",  "name": "New Year\u2019s Day"},
    {"date": "2026-01-19",  "name": "Martin Luther King Jr. Day"},
    {"date": "2026-02-16",  "name": "Presidents\u2019 Day"},
    {"date": "2026-05-25",  "name": "Memorial Day"},
    {"date": "2026-06-19",  "name": "Juneteenth"},
    {"date": "2026-07-04",  "name": "Independence Day"},
    {"date": "2026-09-07",  "name": "Labor Day"},
    {"date": "2026-10-12",  "name": "Indigenous Peoples\u2019 Day"},
    {"date": "2026-11-11",  "name": "Veterans Day"},
    {"date": "2026-11-26",  "name": "Thanksgiving Day"},
    {"date": "2026-12-25",  "name": "Christmas Day"},
]

# hours_by_dow indexed 0=Sun … 6=Sat
WMD_FALLBACK_TRANSFER = {
    "name": "Transfer Station",
    "url":  WMD_URL,
    "schedules": [
        {
            "label":      "Regular",
            "start_date": "",
            "end_date":   "",
            "hours_by_dow": [
                "7:30\u2009AM\u2013\u200912:00\u2009PM",  # Sun
                "7:30\u2009AM\u2013\u20094:00\u2009PM",   # Mon
                "7:30\u2009AM\u2013\u200912:00\u2009PM",  # Tue
                "7:30\u2009AM\u2013\u20094:00\u2009PM",   # Wed
                "7:30\u2009AM\u2013\u200912:00\u2009PM",  # Thu
                "7:30\u2009AM\u2013\u20094:00\u2009PM",   # Fri
                "7:30\u2009AM\u2013\u200912:00\u2009PM",  # Sat
            ],
            "holiday_closings": WMD_HOLIDAY_FALLBACK,
        }
    ],
}

WMD_FALLBACK_COMPOST = {
    "name": "Compost Station",
    "url":  WMD_URL,
    "schedules": [
        {
            "label":      "Regular",
            "start_date": "",
            "end_date":   "",
            "hours_by_dow": [
                "Closed",                                   # Sun
                "7:30\u2009AM\u2013\u20094:00\u2009PM",    # Mon
                "12:30\u2009PM\u2013\u20094:00\u2009PM",   # Tue
                "7:30\u2009AM\u2013\u20094:00\u2009PM",    # Wed
                "12:30\u2009PM\u2013\u20094:00\u2009PM",   # Thu
                "7:30\u2009AM\u2013\u20094:00\u2009PM",    # Fri
                "12:30\u2009PM\u2013\u20094:00\u2009PM",   # Sat
            ],
            "holiday_closings": WMD_HOLIDAY_FALLBACK,
        }
    ],
}

# ── Library (Fishers Island Library) ─────────────────────────────────────────
# Source: https://filibrary.org/
# The page shows a season label ("Spring Hours", "Summer Hours", etc.)
# which changes during the year.  The fallback reflects Spring Hours.
LIBRARY_URL = "https://filibrary.org/"

LIBRARY_FALLBACK = {
    "name": "Library",
    "url":  LIBRARY_URL,
    "schedules": [
        {
            "label":      "Spring Hours",
            "start_date": "",
            "end_date":   "",
            # 0=Sun … 6=Sat
            "hours_by_dow": [
                "Closed",         # Sun
                "1\u20135pm",     # Mon
                "1\u20137pm",     # Tue
                "1\u20135pm",     # Wed
                "1\u20137pm",     # Thu
                "1\u20135pm",     # Fri
                "9am\u201312pm",  # Sat
            ],
        }
    ],
}


# ── Village Market ───────────────────────────────────────────────────────────
# Source: https://fishersisland.net/listing/village-market/
# Page carries two seasonal schedules (Winter / Summer) and announces holiday
# closings with "HOLIDAY HOURS to follow" — no explicit dates, so we use the
# standard US federal holidays as a conservative fallback.
VM_URL = "https://fishersisland.net/listing/village-market/"

VM_HOLIDAY_FALLBACK = [
    {"date": "2026-01-01",  "name": "New Year\u2019s Day"},
    {"date": "2026-05-25",  "name": "Memorial Day"},
    {"date": "2026-07-04",  "name": "Independence Day"},
    {"date": "2026-09-07",  "name": "Labor Day"},
    {"date": "2026-11-26",  "name": "Thanksgiving Day"},
    {"date": "2026-12-25",  "name": "Christmas Day"},
]

# Summer schedule is listed first so getBusinessHours() matches it during the
# summer window before falling through to the always-active Winter schedule.
VM_FALLBACK = {
    "name": "Village Market",
    "url":  VM_URL,
    "schedules": [
        {
            "label":      "Summer",
            "start_date": "2026-06-13",   # approximate; updated by scraper
            "end_date":   "2026-08-30",
            "hours_by_dow": [
                "Closed",                  # Sun
                "7:30AM\u20135:00PM",      # Mon
                "7:30AM\u20135:00PM",      # Tue
                "7:30AM\u20135:00PM",      # Wed
                "7:30AM\u20135:00PM",      # Thu
                "7:30AM\u20135:00PM",      # Fri
                "7:30AM\u20135:00PM",      # Sat
            ],
            "holiday_closings": VM_HOLIDAY_FALLBACK,
        },
        {
            "label":      "Winter",
            "start_date": "",
            "end_date":   "",
            "hours_by_dow": [
                "Closed",                                    # Sun
                "7:30AM\u20131:00PM & 3:00PM\u20135:00PM",  # Mon
                "7:30AM\u20131:00PM & 3:00PM\u20135:00PM",  # Tue
                "7:30AM\u20131:00PM & 3:00PM\u20135:00PM",  # Wed
                "7:30AM\u20131:00PM & 3:00PM\u20135:00PM",  # Thu
                "7:30AM\u20131:00PM & 3:00PM\u20135:00PM",  # Fri
                "7:30AM\u20131:00PM & 3:00PM\u20135:00PM",  # Sat
            ],
            "holiday_closings": VM_HOLIDAY_FALLBACK,
        },
    ],
}


# ════════════════════════════════════════════════════════════════════════════
# Minimal HTML parser  (stdlib only — no bs4 needed)
# ════════════════════════════════════════════════════════════════════════════

class _TableParser(HTMLParser):
    """Walk HTML once; collect (heading_text, [[cell, ...], ...]) pairs."""

    def __init__(self):
        super().__init__()
        self.sections = []          # [{"heading": str, "rows": [[str,…],…]}]
        self._in_h = False
        self._h_text = ""
        self._pending_h = ""
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._cell_buf = []
        self._row_buf = []
        self._table_rows = []

    def handle_starttag(self, tag, _attrs):
        if tag in ("h2","h3","h4","h5","h6"):
            self._in_h = True; self._h_text = ""
        elif tag == "table":
            self._in_table = True; self._table_rows = []
        elif tag == "tr" and self._in_table:
            self._in_row = True; self._row_buf = []
        elif tag in ("td","th") and self._in_row:
            self._in_cell = True; self._cell_buf = []

    def handle_endtag(self, tag):
        if tag in ("h2","h3","h4","h5","h6") and self._in_h:
            self._in_h = False
            self._pending_h = " ".join(self._h_text.split())
        elif tag == "table" and self._in_table:
            self._in_table = False
            self.sections.append({"heading": self._pending_h, "rows": self._table_rows})
            self._table_rows = []
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._row_buf: self._table_rows.append(self._row_buf)
            self._row_buf = []
        elif tag in ("td","th") and self._in_cell:
            self._in_cell = False
            self._row_buf.append(" ".join("".join(self._cell_buf).split()))
            self._cell_buf = []

    def handle_data(self, data):
        if self._in_h:    self._h_text += data
        elif self._in_cell: self._cell_buf.append(data)


# ════════════════════════════════════════════════════════════════════════════
# Date helpers
# ════════════════════════════════════════════════════════════════════════════

_MONTH = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

def _parse_date(s: str) -> str | None:
    s = s.strip().rstrip(".")
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    m = re.match(r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})', s)
    if m:
        mon = _MONTH.get(m.group(1).lower())
        if mon: return f"{m.group(3)}-{mon:02d}-{int(m.group(2)):02d}"
    return None

def _parse_date_range(text: str):
    text = re.sub(r'[–—−]', '-', text)
    m = re.search(
        r'([A-Za-z]+\.?\s+\d{1,2},?\s+\d{4})\s*-\s*([A-Za-z]+\.?\s+\d{1,2},?\s+\d{4})',
        text)
    if m: return _parse_date(m.group(1)), _parse_date(m.group(2))
    return None, None


# ════════════════════════════════════════════════════════════════════════════
# IHP parser
# ════════════════════════════════════════════════════════════════════════════

_DAYS = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"]
_DAY_IDX = {d: i for i, d in enumerate(_DAYS)}

def _normalize_hours(s: str) -> str:
    """Collapse whitespace; normalise dash characters."""
    s = " ".join(s.split())
    s = re.sub(r'\s*[–—−]\s*', '\u2013', s)
    return s

def parse_ihp_html(html: str) -> list | None:
    """Return list of schedule dicts or None."""
    p = _TableParser(); p.feed(html)
    schedules = []
    for sec in p.sections:
        heading = sec["heading"]
        day_rows = {}
        for row in sec["rows"]:
            if not row: continue
            key = row[0].lower().rstrip(":").strip()
            if key in _DAY_IDX and len(row) >= 2:
                day_rows[_DAY_IDX[key]] = _normalize_hours(row[1])
        if not day_rows: continue

        hours_by_dow = [day_rows.get(i, "Closed") for i in range(7)]
        h = heading.lower()
        label = "Peak" if "peak" in h and "off" not in h and "non" not in h else "Off Peak"
        start, end = _parse_date_range(heading)
        schedules.append({
            "label": label,
            "start_date": start or "",
            "end_date":   end   or "",
            "hours_by_dow": hours_by_dow,
        })
    return schedules or None


# ════════════════════════════════════════════════════════════════════════════
# WMD parser
# ════════════════════════════════════════════════════════════════════════════

# Python weekday() 0=Mon … 6=Sun → JS getDay() 0=Sun … 6=Sat
_PY_TO_JS = {0:1, 1:2, 2:3, 3:4, 4:5, 5:6, 6:0}

_FACILITY_ALIASES = {
    "transfer": "Transfer Station",
    "compost":  "Compost Station",
}

def _identify_facility(text: str) -> str | None:
    t = text.lower()
    for key, name in _FACILITY_ALIASES.items():
        if key in t: return name
    return None

def _parse_time_range(s: str) -> str:
    """Normalise a time range string, preserving original spacing."""
    return " ".join(s.split())

def parse_wmd_html(html: str) -> dict | None:
    """
    Try to extract Transfer Station and Compost Station hours from WMD HTML.
    Returns {"Transfer Station": {...biz...}, "Compost Station": {...biz...}}
    or None if parsing fails.
    """
    p = _TableParser(); p.feed(html)

    # ── Strategy 1: separate tables, each under a facility-name heading ──
    facilities: dict[str, list] = {}
    for sec in p.sections:
        fname = _identify_facility(sec["heading"])
        if fname is None: continue
        day_rows = {}
        for row in sec["rows"]:
            if not row: continue
            key = row[0].lower().rstrip(":").strip()
            if key in _DAY_IDX and len(row) >= 2:
                day_rows[_DAY_IDX[key]] = _parse_time_range(row[1])
        if day_rows:
            hours_by_dow = [day_rows.get(i, "Closed") for i in range(7)]
            facilities[fname] = hours_by_dow

    # ── Strategy 2: combined table (Day | Transfer | Compost) ──────────────
    if len(facilities) < 2:
        for sec in p.sections:
            rows = sec["rows"]
            if not rows: continue
            # Detect header row with facility names
            hdr = [c.lower() for c in rows[0]]
            t_col = next((i for i, c in enumerate(hdr) if "transfer" in c), None)
            c_col = next((i for i, c in enumerate(hdr) if "compost"  in c), None)
            if t_col is None and c_col is None: continue
            t_dow: dict[int,str] = {}
            c_dow: dict[int,str] = {}
            for row in rows[1:]:
                if not row: continue
                key = row[0].lower().rstrip(":").strip()
                if key not in _DAY_IDX: continue
                idx = _DAY_IDX[key]
                if t_col and t_col < len(row): t_dow[idx] = _parse_time_range(row[t_col])
                if c_col and c_col < len(row): c_dow[idx] = _parse_time_range(row[c_col])
            if t_dow: facilities["Transfer Station"] = [t_dow.get(i, "Closed") for i in range(7)]
            if c_dow: facilities["Compost Station"]  = [c_dow.get(i, "Closed") for i in range(7)]
            if len(facilities) >= 2: break

    if not facilities:
        return None

    def _make_biz(name: str, hours_by_dow: list) -> dict:
        return {
            "name": name,
            "url":  WMD_URL,
            "schedules": [{
                "label":           "Regular",
                "start_date":      "",
                "end_date":        "",
                "hours_by_dow":    hours_by_dow,
                "holiday_closings": WMD_HOLIDAY_FALLBACK,  # always use known holidays
            }],
        }

    return {n: _make_biz(n, h) for n, h in facilities.items()}


# ════════════════════════════════════════════════════════════════════════════
# Library parser  (filibrary.org — free-text, NOT table-based)
# ════════════════════════════════════════════════════════════════════════════

# Day abbreviations → DOW index (0=Sun … 6=Sat)
# Longer forms listed first so word-boundary regex works correctly
# (e.g. "tues" must not match the shorter "tue" alias).
_LIB_DAY_MAP: dict[str, int] = {
    "monday": 1,    "mon": 1,
    "tuesday": 2,   "tues": 2,  "tue": 2,
    "wednesday": 3, "wed": 3,
    "thursday": 4,  "thurs": 4, "thu": 4,
    "friday": 5,    "fri": 5,
    "saturday": 6,  "sat": 6,
    "sunday": 0,    "sun": 0,
}

_LIB_SEASON_PAT = re.compile(
    r'\b(spring|summer|fall|autumn|winter|year[- ]?round)\s+hours?\b',
    re.IGNORECASE,
)

_LIB_TIME_PAT = re.compile(
    r'\d{1,2}(?::\d{2})?\s*(?:am|pm)?\s*[-\u2013\u2014]\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)',
    re.IGNORECASE,
)


def parse_library_html(html: str) -> dict | None:
    """
    Parse filibrary.org free-text HTML.

    The page uses a structure like:
        <em>Spring Hours:</em>
        <strong>Mon, Wed, Fri</strong> 1-5pm | <strong>Tues, Thurs</strong> 1-7pm
        <strong>Sat</strong> 9am-12pm
        <strong>Sunday</strong> Closed unless for scheduled event.

    Returns a business dict (ready for hours.json) or None on failure.
    Works for any season label the page might carry (Spring, Summer, …).
    """
    from html import unescape as _unescape

    # Replace <br> variants with newlines before stripping tags
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    # Strip all remaining HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    text = _unescape(text)

    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    # Locate the first season label
    start_idx = None
    season_label = "Season Hours"
    for i, line in enumerate(lines):
        m = _LIB_SEASON_PAT.search(line)
        if m:
            start_idx = i
            season_label = m.group(0).title()
            break

    if start_idx is None:
        return None

    # Collect up to 12 lines after the season label as the schedule block;
    # stop early if we hit another season label.
    schedule_lines: list[str] = []
    for line in lines[start_idx + 1 : start_idx + 13]:
        if _LIB_SEASON_PAT.search(line):
            break
        schedule_lines.append(line)

    dow: dict[int, str] = {}

    for line in schedule_lines:
        # Split on pipe: "Mon, Wed, Fri 1-5pm | Tues, Thurs 1-7pm"
        for part in line.split('|'):
            part = part.strip()
            if not part:
                continue

            # Find all day-of-week indices mentioned in this segment.
            found: list[int] = []
            for abbrev, idx in _LIB_DAY_MAP.items():
                if idx in found:
                    continue
                if re.search(r'\b' + re.escape(abbrev) + r'\b', part, re.IGNORECASE):
                    found.append(idx)

            if not found:
                continue

            # Determine the time value for these days.
            time_m = _LIB_TIME_PAT.search(part)
            if time_m:
                raw = time_m.group(0)
                raw = re.sub(r'\s+', '', raw)              # strip internal spaces
                raw = re.sub(r'[-\u2013\u2014]', '\u2013', raw)  # normalise to en-dash
                time_val = raw
            elif re.search(r'\bclosed\b', part, re.IGNORECASE):
                time_val = "Closed"
            else:
                continue  # unrecognised format; skip segment

            for d in found:
                if d not in dow:  # first occurrence wins
                    dow[d] = time_val

    if not dow:
        return None

    hours_by_dow = [dow.get(i, "Closed") for i in range(7)]

    return {
        "name": "Library",
        "url":  LIBRARY_URL,
        "schedules": [{
            "label":        season_label,
            "start_date":   "",
            "end_date":     "",
            "hours_by_dow": hours_by_dow,
        }],
    }


# ════════════════════════════════════════════════════════════════════════════
# Village Market parser  (fishersisland.net listing — table-based)
# ════════════════════════════════════════════════════════════════════════════

_VM_SEASON_PAT = re.compile(
    r'\b(summer|winter|spring|fall|autumn|year[- ]?round)\b\s*hours?',
    re.IGNORECASE,
)

# Day abbreviations used in the VM table headers → DOW index (0=Sun … 6=Sat)
_VM_DAY_ABBR: dict[str, int] = {
    "SUN": 0,  "SUNDAY": 0,
    "MON": 1,  "MONDAY": 1,
    "TUE": 2,  "TUES": 2,  "TUESDAY": 2,
    "WED": 3,  "WEDNESDAY": 3,
    "THU": 4,  "THURS": 4, "THURSDAY": 4,
    "FRI": 5,  "FRIDAY": 5,
    "SAT": 6,  "SATURDAY": 6,
}


def _expand_vm_days(cell: str) -> list[int]:
    """
    Expand a day-range cell string to a list of DOW indices.
    "MON-SAT" → [1,2,3,4,5,6];  "SUNDAY" → [0];  "MON" → [1]; etc.
    Returns [] if the cell doesn't look like a day name or range.
    """
    # Strip non-word chars (handles en-dash, hyphen, commas …)
    tokens = re.sub(r'[^\w]+', ' ', cell.strip().upper()).split()
    if not tokens:
        return []
    a = _VM_DAY_ABBR.get(tokens[0])
    if a is None:
        return []
    if len(tokens) == 1:
        return [a]
    b = _VM_DAY_ABBR.get(tokens[-1])
    if b is None:
        return [a]
    # Expand range a..b (handles Sun=0 wrapping if needed)
    if a <= b:
        return list(range(a, b + 1))
    return list(range(a, 7)) + list(range(0, b + 1))


def _normalize_vm_time(s: str) -> str | None:
    """
    Return a normalised time string, "Closed", or None if not time-like.
    """
    s = " ".join(s.split())
    if re.search(r'\bclosed\b', s, re.IGNORECASE):
        return "Closed"
    if not re.search(r'\d{1,2}(?::\d{2})?\s*[AaPp][Mm]', s):
        return None  # not a time value (e.g. "Papers available…")
    # Normalise surrounding whitespace on dashes/en-dashes
    return re.sub(r'\s*[-\u2013\u2014]\s*', '\u2013', s)


def _vm_parse_yearless_range(text: str):
    """
    Parse a date range that lacks an explicit year, e.g.
    "SATURDAY, JUNE 17 – SATURDAY, AUGUST 30".
    Infers the year: uses the current year unless that start date is more
    than 60 days in the past (in which case uses next year).
    Returns (start_YYYY-MM-DD, end_YYYY-MM-DD) or (None, None).
    """
    text = re.sub(r'[\u2013\u2014\u2212]', '-', text)   # normalise dashes
    # Match: optional "DAYOFWEEK," MONTH DAY - optional "DAYOFWEEK," MONTH DAY
    m = re.search(
        r'(?:[A-Za-z]+,?\s+)?([A-Za-z]+)\s+(\d{1,2})'   # start: MONTH DAY
        r'\s*-\s*'
        r'(?:[A-Za-z]+,?\s+)?([A-Za-z]+)\s+(\d{1,2})',  # end:   MONTH DAY
        text,
    )
    if not m:
        return None, None
    sm = _MONTH.get(m.group(1).lower())
    sd = int(m.group(2))
    em = _MONTH.get(m.group(3).lower())
    ed = int(m.group(4))
    if not sm or not em:
        return None, None

    today = datetime.now(timezone.utc).date()
    yr = today.year
    try:
        cand = date(yr, sm, sd)
        if (cand - today).days < -60:
            yr += 1
        end_yr = yr if em >= sm else yr + 1
        return date(yr, sm, sd).strftime("%Y-%m-%d"), date(end_yr, em, ed).strftime("%Y-%m-%d")
    except ValueError:
        return None, None


def parse_village_market_html(html: str) -> dict | None:
    """
    Parse Village Market seasonal hours from the fishersisland.net listing page.

    Page structure (h5 heading → table):
      "WINTER HOURS AS OF TUESDAY, SEPTEMBER 2"
        MON-SAT  | 7:30 AM – 1:00 PM
                 | 3:00 PM – 5:00 PM    ← continuation row
        SUNDAY   | CLOSED

      "SUMMER HOURS"
                 | SATURDAY, JUNE 17 – SATURDAY, AUGUST 30   ← date-range row
        MON-SAT  | 7:30 AM – 5:00 PM
        SUNDAY   | CLOSED

    Returns a business dict or None on failure.
    Summer schedule is placed first so it takes priority during its date window.
    """
    p = _TableParser()
    p.feed(html)

    parsed: dict[str, dict] = {}   # label → schedule dict

    for sec in p.sections:
        heading = sec["heading"]
        sm = _VM_SEASON_PAT.search(heading)
        if not sm:
            continue
        label = sm.group(1).capitalize()   # "Summer", "Winter", …
        rows = sec["rows"]
        if not rows:
            continue

        # ── Extract date range ──────────────────────────────────────────────
        start_date = end_date = ""

        # Try rows first: a row with an empty first cell and a date-like second
        # cell is a date-range row (e.g. summer's "SATURDAY, JUNE 17 – …").
        for row in rows:
            if len(row) < 2:
                continue
            cell0, cell1 = row[0].strip(), row[1].strip()
            if not cell0 or not _expand_vm_days(cell0):
                s, e = _vm_parse_yearless_range(cell1)
                if not s:
                    s, e = _parse_date_range(cell1)  # fallback: try full-year format
                if s:
                    start_date, end_date = s, (e or "")
                    break   # found it

        # ── Parse day-hours ─────────────────────────────────────────────────
        dow: dict[int, str] = {}
        last_days: list[int] = []

        for row in rows:
            if len(row) < 2:
                continue
            cell0, cell1 = row[0].strip(), row[1].strip()

            if cell0:
                days = _expand_vm_days(cell0)
                if not days:
                    continue   # date-range or header row — skip
                last_days = days
                time_val = _normalize_vm_time(cell1)
                if time_val:
                    for d in days:
                        dow[d] = time_val
            else:
                # Continuation row: appends another time slot to the same days
                if last_days:
                    time_val = _normalize_vm_time(cell1)
                    if time_val and time_val != "Closed":
                        for d in last_days:
                            if d in dow:
                                dow[d] = f"{dow[d]} & {time_val}"
                            else:
                                dow[d] = time_val

        if not dow:
            continue

        parsed[label] = {
            "label":            label,
            "start_date":       start_date,
            "end_date":         end_date,
            "hours_by_dow":     [dow.get(i, "Closed") for i in range(7)],
            "holiday_closings": VM_HOLIDAY_FALLBACK,
        }

    if not parsed:
        return None

    # Schedules with explicit date ranges go first so they shadow the
    # always-active Winter fallback during their window.
    schedules = sorted(
        parsed.values(),
        key=lambda s: (not bool(s["start_date"]), s["label"]),
    )

    return {
        "name":      "Village Market",
        "url":       VM_URL,
        "schedules": schedules,
    }


# ════════════════════════════════════════════════════════════════════════════
# Playwright fetch
# ════════════════════════════════════════════════════════════════════════════

async def _fetch(url: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"  → GET {url}", flush=True)
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)
        html = await page.content()
        await browser.close()
        return html


# ════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════

def main():
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    businesses = []

    # ── Transfer Station + Compost Station (fiwmd.net) ───────────────────────
    print("Fetching WMD (Transfer + Compost) …", flush=True)
    wmd_fallback = False
    try:
        html = asyncio.run(_fetch(WMD_URL))
        parsed = parse_wmd_html(html)
        if parsed:
            print(f"  ✓ WMD facilities parsed: {list(parsed.keys())}", flush=True)
            for name in ("Transfer Station", "Compost Station"):
                if name in parsed:
                    businesses.append(parsed[name])
                else:
                    print(f"  ⚠ {name} missing from parse — using fallback", flush=True)
                    businesses.append(
                        WMD_FALLBACK_TRANSFER if "Transfer" in name else WMD_FALLBACK_COMPOST
                    )
                    wmd_fallback = True
        else:
            raise ValueError("no WMD facilities parsed")
    except Exception as e:
        print(f"  ⚠ WMD failed ({e}) — using fallback", flush=True)
        wmd_fallback = True
        businesses.append(WMD_FALLBACK_TRANSFER)
        businesses.append(WMD_FALLBACK_COMPOST)

    # ── Fishers Island Freight Office (hardcoded, below Compost) ─────────────
    print("Adding Fishers Island Freight Office (hardcoded hours) …", flush=True)
    businesses.append(FREIGHT_OFFICE_BUSINESS)

    # ── Library (filibrary.org) ───────────────────────────────────────────────
    print("Fetching Library (filibrary.org) …", flush=True)
    lib_fallback = False
    try:
        html = asyncio.run(_fetch(LIBRARY_URL))
        biz = parse_library_html(html)
        if biz:
            print(f"  ✓ Library hours parsed: {biz['schedules'][0]['label']}", flush=True)
            businesses.append(biz)
        else:
            raise ValueError("no library hours parsed")
    except Exception as e:
        print(f"  ⚠ Library failed ({e}) — using fallback", flush=True)
        lib_fallback = True
        businesses.append(LIBRARY_FALLBACK)

    # ── Village Market (fishersisland.net) ───────────────────────────────────
    print("Fetching Village Market (fishersisland.net) …", flush=True)
    vm_fallback = False
    try:
        html = asyncio.run(_fetch(VM_URL))
        biz = parse_village_market_html(html)
        if biz:
            labels = [s["label"] for s in biz["schedules"]]
            print(f"  ✓ Village Market schedules parsed: {labels}", flush=True)
            businesses.append(biz)
        else:
            raise ValueError("no Village Market schedules parsed")
    except Exception as e:
        print(f"  ⚠ Village Market failed ({e}) — using fallback", flush=True)
        vm_fallback = True
        businesses.append(VM_FALLBACK)

    # ── Beach Plum (hardcoded) ────────────────────────────────────────────────
    print("Adding Beach Plum (hardcoded hours) …", flush=True)
    businesses.append(BEACH_PLUM_BUSINESS)

    # ── West End Cafe (hardcoded) ─────────────────────────────────────────────
    print("Adding West End Cafe (hardcoded hours) …", flush=True)
    businesses.append(WEST_END_BUSINESS)

    # ── Doctor's Office (last) ────────────────────────────────────────────────
    print("Fetching IHP (Doctor's Office) …", flush=True)
    ihp_fallback = False
    try:
        html = asyncio.run(_fetch(IHP_URL))
        schedules = parse_ihp_html(html)
        if schedules:
            print(f"  ✓ {len(schedules)} IHP schedule(s) parsed from live page", flush=True)
            businesses.append({
                "name":      "Doctor's Office",
                "url":       IHP_URL,
                "schedules": schedules,
            })
        else:
            raise ValueError("no schedules parsed")
    except Exception as e:
        print(f"  ⚠ IHP failed ({e}) — using fallback", flush=True)
        ihp_fallback = True
        businesses.append({
            "name":      "Doctor's Office",
            "url":       IHP_URL,
            "schedules": IHP_FALLBACK_SCHEDULES,
        })

    # Normalise all hours_by_dow strings regardless of source
    for biz in businesses:
        for sched in biz.get("schedules", []):
            sched["hours_by_dow"] = [
                normalize_hours(h) for h in sched["hours_by_dow"]
            ]

    data = {
        "fetched_at":    now_iso,
        "used_fallback": ihp_fallback or wmd_fallback or lib_fallback or vm_fallback,
        "businesses":    businesses,
    }

    with open("hours.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("✓ hours.json written", flush=True)


if __name__ == "__main__":
    main()
