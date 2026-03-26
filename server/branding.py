"""Centralized branding and geography config.

Change this file to rebrand the app for a different energy distributor or country.
All other files import from here — no hardcoded company/location references elsewhere.
"""

from datetime import date

# ── Company Identity ─────────────────────────────────────────────────────────

COMPANY_NAME = "Regional Energy"
COMPANY_SHORT = "RE"
INDUSTRY = "electricity distribution network operator"
COMPANY_DOMAIN = "regionalenergy.com.au"
COMPANY_OUTAGE_URL = "https://www.regionalenergy.com.au/outages"

# ── App Identity ─────────────────────────────────────────────────────────────

APP_TITLE = "Energy Crew Briefing"
APP_DISPLAY = "Field Operations Briefing Assistant"
APP_SUBTITLE = "Powered by Databricks AI"
PAGE_TITLE = f"{COMPANY_NAME} — Field Operations Briefing"

# ── Geography ────────────────────────────────────────────────────────────────

COUNTRY = "Australia"
STATE = "New South Wales"
STATE_SHORT = "NSW"
TIMEZONE = "Australia/Sydney"
TIMEZONE_LABEL = "AEST"

# ── Colors (CSS hex) ────────────────────────────────────────────────────────

COLOR_PRIMARY = "#1B4F72"
COLOR_PRIMARY_LIGHT = "#21618C"
COLOR_ACCENT = "#E67E22"
COLOR_ACCENT_LIGHT = "#FEF5E7"
COLOR_USER_BG = "#1B4F72"
COLOR_PDF_PRIMARY = "#1a4731"
COLOR_PDF_ACCENT = "#E67E22"

# ── Unity Catalog ────────────────────────────────────────────────────────────

UC_CATALOG = "main"
UC_SCHEMA = "energy_crew_briefing"
UC_FULL = f"{UC_CATALOG}.{UC_SCHEMA}"

# ── Depots (lat/lon for weather + web search) ───────────────────────────────

DEPOTS = {
    "grafton":        {"name": "Grafton",         "lat": -29.69, "lon": 152.93, "region": "Northern Rivers"},
    "coffs harbour":  {"name": "Coffs Harbour",   "lat": -30.30, "lon": 153.11, "region": "Mid North Coast"},
    "tamworth":       {"name": "Tamworth",         "lat": -31.09, "lon": 150.93, "region": "New England"},
    "orange":         {"name": "Orange",           "lat": -33.28, "lon": 149.10, "region": "Central West"},
    "dubbo":          {"name": "Dubbo",            "lat": -32.25, "lon": 148.60, "region": "Orana"},
    "wagga wagga":    {"name": "Wagga Wagga",      "lat": -35.12, "lon": 147.37, "region": "Riverina"},
    "armidale":       {"name": "Armidale",         "lat": -30.51, "lon": 151.67, "region": "New England"},
    "port macquarie": {"name": "Port Macquarie",   "lat": -31.43, "lon": 152.91, "region": "Mid North Coast"},
    "bathurst":       {"name": "Bathurst",         "lat": -33.42, "lon": 149.58, "region": "Central West"},
    "broken hill":    {"name": "Broken Hill",      "lat": -31.95, "lon": 141.47, "region": "Far West"},
    "lismore":        {"name": "Lismore",          "lat": -28.81, "lon": 153.28, "region": "Northern Rivers"},
    "casino":         {"name": "Casino",           "lat": -28.87, "lon": 153.05, "region": "Northern Rivers"},
    "glen innes":     {"name": "Glen Innes",       "lat": -29.73, "lon": 151.74, "region": "New England"},
    "inverell":       {"name": "Inverell",         "lat": -29.78, "lon": 151.11, "region": "New England"},
    "mudgee":         {"name": "Mudgee",           "lat": -32.59, "lon": 149.59, "region": "Central West"},
    "moree":          {"name": "Moree",            "lat": -29.46, "lon": 149.85, "region": "North West"},
    "lightning ridge": {"name": "Lightning Ridge", "lat": -29.43, "lon": 147.98, "region": "North West"},
    "queanbeyan":     {"name": "Queanbeyan",       "lat": -35.35, "lon": 149.23, "region": "South East"},
    "bega":           {"name": "Bega",             "lat": -36.67, "lon": 149.84, "region": "South East"},
}

DEPOT_ALIASES = {
    "coffs": "coffs harbour",
    "wagga": "wagga wagga",
    "port": "port macquarie",
}

# ── Council / LGA Mappings (for web search) ─────────────────────────────────

DEPOT_COUNCILS = {
    "grafton":        ("Grafton NSW",         "clarence.nsw.gov.au",         "Clarence Valley"),
    "coffs harbour":  ("Coffs Harbour NSW",   "coffsharbour.nsw.gov.au",     "Coffs Harbour"),
    "port macquarie": ("Port Macquarie NSW",  "pmhc.nsw.gov.au",            "Port Macquarie-Hastings"),
    "taree":          ("Taree NSW",           "midcoast.nsw.gov.au",         "MidCoast"),
    "tamworth":       ("Tamworth NSW",        "tamworth.nsw.gov.au",         "Tamworth Regional"),
    "armidale":       ("Armidale NSW",        "armidaleregional.nsw.gov.au", "Armidale Regional"),
    "orange":         ("Orange NSW",          "orange.nsw.gov.au",           "Orange"),
    "bathurst":       ("Bathurst NSW",        "bathurst.nsw.gov.au",         "Bathurst Regional"),
    "dubbo":          ("Dubbo NSW",           "dubbo.nsw.gov.au",            "Dubbo Regional"),
    "wagga wagga":    ("Wagga Wagga NSW",     "wagga.nsw.gov.au",            "Wagga Wagga"),
    "queanbeyan":     ("Queanbeyan NSW",      "qprc.nsw.gov.au",             "Queanbeyan-Palerang"),
    "bega":           ("Bega NSW",            "begavalley.nsw.gov.au",       "Bega Valley"),
    "broken hill":    ("Broken Hill NSW",     "brokenhill.nsw.gov.au",       "Broken Hill"),
    "lightning ridge": ("Lightning Ridge NSW", "walgett.nsw.gov.au",          "Walgett"),
    "glen innes":     ("Glen Innes NSW",      "gisc.nsw.gov.au",             "Glen Innes Severn"),
    "lismore":        ("Lismore NSW",         "lismore.nsw.gov.au",          "Lismore"),
    "casino":         ("Casino NSW",          "richmondvalley.nsw.gov.au",   "Richmond Valley"),
    "inverell":       ("Inverell NSW",        "inverell.nsw.gov.au",         "Inverell"),
    "mudgee":         ("Mudgee NSW",          "midwestern.nsw.gov.au",       "Mid-Western Regional"),
    "moree":          ("Moree NSW",           "mpsc.nsw.gov.au",             "Moree Plains"),
}

WEB_SEARCH_DOMAINS = [
    "livetraffic.com",
    "transport.nsw.gov.au",
    COMPANY_DOMAIN,
    "ses.nsw.gov.au",
    "rfs.nsw.gov.au",
]

# ── Crews ────────────────────────────────────────────────────────────────────

CREWS = {
    "Grafton Lines A":          {"depot": "Grafton",         "type": "Planned Maintenance"},
    "Grafton Lines B":          {"depot": "Grafton",         "type": "Corrective Maintenance"},
    "Coffs Harbour Lines":      {"depot": "Coffs Harbour",   "type": "Planned Maintenance"},
    "Coffs Harbour Cable":      {"depot": "Coffs Harbour",   "type": "Capital Works"},
    "Lismore Lines":            {"depot": "Lismore",         "type": "Corrective Maintenance"},
    "Port Macquarie Lines":     {"depot": "Port Macquarie",  "type": "Planned Maintenance"},
    "Tamworth Lines":           {"depot": "Tamworth",        "type": "Planned Maintenance"},
    "Tamworth Substation":      {"depot": "Tamworth",        "type": "Capital Works"},
    "Armidale Lines":           {"depot": "Armidale",        "type": "Planned Maintenance"},
    "Armidale Inspection":      {"depot": "Armidale",        "type": "Inspection"},
    "Orange Lines":             {"depot": "Orange",          "type": "Planned Maintenance"},
    "Orange Cable":             {"depot": "Orange",          "type": "Capital Works"},
    "Dubbo Lines":              {"depot": "Dubbo",           "type": "Planned Maintenance"},
    "Dubbo Emergency":          {"depot": "Dubbo",           "type": "Emergency Response"},
    "Bathurst Lines":           {"depot": "Bathurst",        "type": "Planned Maintenance"},
    "Wagga Wagga Lines":        {"depot": "Wagga Wagga",     "type": "Planned Maintenance"},
    "Wagga Wagga Inspection":   {"depot": "Wagga Wagga",     "type": "Inspection"},
    "Broken Hill Lines":        {"depot": "Broken Hill",     "type": "Planned Maintenance"},
    "Moree Lines":              {"depot": "Moree",           "type": "Planned Maintenance"},
    "Mudgee Lines":             {"depot": "Mudgee",          "type": "Planned Maintenance"},
    "Inverell Lines":           {"depot": "Inverell",        "type": "Planned Maintenance"},
    "Queanbeyan Lines":         {"depot": "Queanbeyan",      "type": "Planned Maintenance"},
    "Contractor Downer":        {"depot": "Various",         "type": "Capital Works"},
    "Contractor Asplundh":      {"depot": "Various",         "type": "Vegetation Management"},
    "Contractor Fulton Hogan":  {"depot": "Various",         "type": "Capital Works"},
}

CREW_LIST_STRING = ", ".join(CREWS.keys())

LOCATION_CREWS = {
    "grafton":        ["Grafton Lines A", "Grafton Lines B"],
    "coffs harbour":  ["Coffs Harbour Lines", "Coffs Harbour Cable"],
    "tamworth":       ["Tamworth Lines", "Tamworth Substation"],
    "orange":         ["Orange Lines", "Orange Cable"],
    "dubbo":          ["Dubbo Lines", "Dubbo Emergency"],
    "wagga wagga":    ["Wagga Wagga Lines", "Wagga Wagga Inspection"],
    "armidale":       ["Armidale Lines", "Armidale Inspection"],
    "port macquarie": ["Port Macquarie Lines"],
    "bathurst":       ["Bathurst Lines"],
    "broken hill":    ["Broken Hill Lines"],
    "lismore":        ["Lismore Lines"],
    "inverell":       ["Inverell Lines"],
    "mudgee":         ["Mudgee Lines"],
    "moree":          ["Moree Lines"],
    "queanbeyan":     ["Queanbeyan Lines"],
    "bega":           [],
    "casino":         [],
    "glen innes":     [],
    "lightning ridge": [],
}

# ── BOM Weather Stations ────────────────────────────────────────────────────

BOM_STATIONS = {
    "Grafton":        {"wmo_id": 94791, "product": "IDN60901"},
    "Coffs Harbour":  {"wmo_id": 59040, "product": "IDN60901"},
    "Tamworth":       {"wmo_id": 94776, "product": "IDN60901"},
    "Orange":         {"wmo_id": 94753, "product": "IDN60901"},
    "Dubbo":          {"wmo_id": 95719, "product": "IDN60901"},
    "Wagga Wagga":    {"wmo_id": 94749, "product": "IDN60901"},
    "Armidale":       {"wmo_id": 94774, "product": "IDN60901"},
    "Port Macquarie": {"wmo_id": 94786, "product": "IDN60901"},
    "Bathurst":       {"wmo_id": 94729, "product": "IDN60901"},
    "Broken Hill":    {"wmo_id": 94689, "product": "IDN60901"},
}

# Open-Meteo stations (superset — includes all depot locations for forecasts)
WEATHER_STATIONS = [
    {"name": d["name"], "lat": d["lat"], "lon": d["lon"]}
    for d in DEPOTS.values()
]
# Deduplicate by name (aliases point to same location)
_seen = set()
WEATHER_STATIONS = [s for s in WEATHER_STATIONS if s["name"] not in _seen and not _seen.add(s["name"])]

# ── Public Holidays ──────────────────────────────────────────────────────────

EASTER_DATES = {date(2026, 4, 3), date(2026, 4, 4), date(2026, 4, 5), date(2026, 4, 6)}
PUBLIC_HOLIDAYS = EASTER_DATES | {
    date(2026, 3, 2),  # Bank Holiday (regional)
}
HOLIDAY_NOTE = "Easter 2026 is 3-6 April (Good Friday to Easter Monday) — no planned work over Easter."
