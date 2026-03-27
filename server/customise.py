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
STATE = "Queensland"
STATE_SHORT = "QLD"
TIMEZONE = "Australia/Brisbane"
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
    "townsville":     {"name": "Townsville",     "lat": -19.25, "lon": 146.80, "region": "North Queensland"},
    "cairns":         {"name": "Cairns",          "lat": -16.92, "lon": 145.77, "region": "Far North Queensland"},
    "mackay":         {"name": "Mackay",          "lat": -21.14, "lon": 149.19, "region": "North Queensland"},
    "rockhampton":    {"name": "Rockhampton",     "lat": -23.38, "lon": 150.51, "region": "Central Queensland"},
    "bundaberg":      {"name": "Bundaberg",       "lat": -24.87, "lon": 152.35, "region": "Wide Bay"},
    "gladstone":      {"name": "Gladstone",       "lat": -23.85, "lon": 151.27, "region": "Central Queensland"},
    "maryborough":    {"name": "Maryborough",     "lat": -25.53, "lon": 152.70, "region": "Wide Bay"},
    "toowoomba":      {"name": "Toowoomba",       "lat": -27.56, "lon": 151.95, "region": "Darling Downs"},
    "roma":           {"name": "Roma",             "lat": -26.57, "lon": 148.79, "region": "Western Queensland"},
    "emerald":        {"name": "Emerald",          "lat": -23.53, "lon": 148.16, "region": "Central Highlands"},
    "mount isa":      {"name": "Mount Isa",        "lat": -20.73, "lon": 139.49, "region": "North West Queensland"},
    "longreach":      {"name": "Longreach",        "lat": -23.44, "lon": 144.25, "region": "Central West Queensland"},
    "gympie":         {"name": "Gympie",           "lat": -26.19, "lon": 152.67, "region": "Wide Bay"},
    "innisfail":      {"name": "Innisfail",        "lat": -17.52, "lon": 146.03, "region": "Far North Queensland"},
    "bowen":          {"name": "Bowen",            "lat": -20.01, "lon": 148.24, "region": "North Queensland"},
    "charters towers": {"name": "Charters Towers", "lat": -20.08, "lon": 146.26, "region": "North Queensland"},
    "atherton":       {"name": "Atherton",         "lat": -17.27, "lon": 145.48, "region": "Tablelands"},
    "ayr":            {"name": "Ayr",              "lat": -19.57, "lon": 147.40, "region": "North Queensland"},
    "biloela":        {"name": "Biloela",          "lat": -24.40, "lon": 150.51, "region": "Central Queensland"},
}

DEPOT_ALIASES = {
    "mt isa": "mount isa",
    "rocky": "rockhampton",
    "bundy": "bundaberg",
}

# ── Council / LGA Mappings (for web search) ─────────────────────────────────

DEPOT_COUNCILS = {
    "townsville":      (f"Townsville {STATE_SHORT}",      "townsville.qld.gov.au",       "Townsville"),
    "cairns":          (f"Cairns {STATE_SHORT}",           "cairns.qld.gov.au",           "Cairns Regional"),
    "mackay":          (f"Mackay {STATE_SHORT}",           "mackay.qld.gov.au",           "Mackay Regional"),
    "rockhampton":     (f"Rockhampton {STATE_SHORT}",      "rockhampton.qld.gov.au",      "Rockhampton Regional"),
    "bundaberg":       (f"Bundaberg {STATE_SHORT}",        "bundaberg.qld.gov.au",        "Bundaberg Regional"),
    "gladstone":       (f"Gladstone {STATE_SHORT}",        "gladstone.qld.gov.au",        "Gladstone Regional"),
    "maryborough":     (f"Maryborough {STATE_SHORT}",      "frasercoast.qld.gov.au",      "Fraser Coast"),
    "toowoomba":       (f"Toowoomba {STATE_SHORT}",        "tr.qld.gov.au",               "Toowoomba Regional"),
    "roma":            (f"Roma {STATE_SHORT}",              "maranoa.qld.gov.au",          "Maranoa Regional"),
    "emerald":         (f"Emerald {STATE_SHORT}",           "chrc.qld.gov.au",             "Central Highlands"),
    "mount isa":       (f"Mount Isa {STATE_SHORT}",         "mountisa.qld.gov.au",         "Mount Isa"),
    "longreach":       (f"Longreach {STATE_SHORT}",         "longreach.qld.gov.au",        "Longreach Regional"),
    "gympie":          (f"Gympie {STATE_SHORT}",            "gympie.qld.gov.au",           "Gympie Regional"),
    "innisfail":       (f"Innisfail {STATE_SHORT}",         "cassowarycoast.qld.gov.au",   "Cassowary Coast"),
    "bowen":           (f"Bowen {STATE_SHORT}",             "whitsundayrc.qld.gov.au",     "Whitsunday"),
    "charters towers":  (f"Charters Towers {STATE_SHORT}",  "charterstowers.qld.gov.au",   "Charters Towers"),
    "atherton":        (f"Atherton {STATE_SHORT}",          "trc.qld.gov.au",              "Tablelands Regional"),
    "ayr":             (f"Ayr {STATE_SHORT}",               "burdekin.qld.gov.au",         "Burdekin"),
    "biloela":         (f"Biloela {STATE_SHORT}",           "banana.qld.gov.au",           "Banana"),
}

WEB_SEARCH_DOMAINS = [
    "qldtraffic.qld.gov.au",
    "tmr.qld.gov.au",
    COMPANY_DOMAIN,
    "ses.qld.gov.au",
    "qfes.qld.gov.au",
]

# ── Crews ────────────────────────────────────────────────────────────────────

CREWS = {
    "Townsville Lines A":       {"depot": "Townsville",      "type": "Planned Maintenance"},
    "Townsville Lines B":       {"depot": "Townsville",      "type": "Corrective Maintenance"},
    "Cairns Lines":             {"depot": "Cairns",           "type": "Planned Maintenance"},
    "Cairns Cable":             {"depot": "Cairns",           "type": "Capital Works"},
    "Mackay Lines":             {"depot": "Mackay",           "type": "Planned Maintenance"},
    "Rockhampton Lines":        {"depot": "Rockhampton",      "type": "Planned Maintenance"},
    "Rockhampton Substation":   {"depot": "Rockhampton",      "type": "Capital Works"},
    "Bundaberg Lines":          {"depot": "Bundaberg",        "type": "Planned Maintenance"},
    "Bundaberg Inspection":     {"depot": "Bundaberg",        "type": "Inspection"},
    "Gladstone Lines":          {"depot": "Gladstone",        "type": "Planned Maintenance"},
    "Toowoomba Lines":          {"depot": "Toowoomba",        "type": "Planned Maintenance"},
    "Toowoomba Cable":          {"depot": "Toowoomba",        "type": "Capital Works"},
    "Roma Lines":               {"depot": "Roma",              "type": "Planned Maintenance"},
    "Roma Emergency":           {"depot": "Roma",              "type": "Emergency Response"},
    "Emerald Lines":            {"depot": "Emerald",           "type": "Planned Maintenance"},
    "Mount Isa Lines":          {"depot": "Mount Isa",         "type": "Planned Maintenance"},
    "Mount Isa Inspection":     {"depot": "Mount Isa",         "type": "Inspection"},
    "Longreach Lines":          {"depot": "Longreach",         "type": "Planned Maintenance"},
    "Innisfail Lines":          {"depot": "Innisfail",         "type": "Corrective Maintenance"},
    "Charters Towers Lines":    {"depot": "Charters Towers",   "type": "Planned Maintenance"},
    "Atherton Lines":           {"depot": "Atherton",          "type": "Planned Maintenance"},
    "Gympie Lines":             {"depot": "Gympie",            "type": "Planned Maintenance"},
    "Contractor Downer":        {"depot": "Various",           "type": "Capital Works"},
    "Contractor Asplundh":      {"depot": "Various",           "type": "Vegetation Management"},
    "Contractor Fulton Hogan":  {"depot": "Various",           "type": "Capital Works"},
}

CREW_LIST_STRING = ", ".join(CREWS.keys())

LOCATION_CREWS = {
    "townsville":      ["Townsville Lines A", "Townsville Lines B"],
    "cairns":          ["Cairns Lines", "Cairns Cable"],
    "mackay":          ["Mackay Lines"],
    "rockhampton":     ["Rockhampton Lines", "Rockhampton Substation"],
    "bundaberg":       ["Bundaberg Lines", "Bundaberg Inspection"],
    "gladstone":       ["Gladstone Lines"],
    "toowoomba":       ["Toowoomba Lines", "Toowoomba Cable"],
    "roma":            ["Roma Lines", "Roma Emergency"],
    "emerald":         ["Emerald Lines"],
    "mount isa":       ["Mount Isa Lines", "Mount Isa Inspection"],
    "longreach":       ["Longreach Lines"],
    "innisfail":       ["Innisfail Lines"],
    "charters towers": ["Charters Towers Lines"],
    "atherton":        ["Atherton Lines"],
    "gympie":          ["Gympie Lines"],
    "maryborough":     [],
    "bowen":           [],
    "ayr":             [],
    "biloela":         [],
}

# ── BOM Weather Stations ────────────────────────────────────────────────────

BOM_STATIONS = {
    "Townsville":      {"wmo_id": 94294, "product": "IDQ60901"},
    "Cairns":          {"wmo_id": 94287, "product": "IDQ60901"},
    "Mackay":          {"wmo_id": 94367, "product": "IDQ60901"},
    "Rockhampton":     {"wmo_id": 94374, "product": "IDQ60901"},
    "Bundaberg":       {"wmo_id": 94387, "product": "IDQ60901"},
    "Gladstone":       {"wmo_id": 94387, "product": "IDQ60901"},
    "Toowoomba":       {"wmo_id": 94556, "product": "IDQ60901"},
    "Roma":            {"wmo_id": 94515, "product": "IDQ60901"},
    "Emerald":         {"wmo_id": 94363, "product": "IDQ60901"},
    "Mount Isa":       {"wmo_id": 94326, "product": "IDQ60901"},
}

# Open-Meteo stations (superset — includes all depot locations for forecasts)
WEATHER_STATIONS = [
    {"name": d["name"], "lat": d["lat"], "lon": d["lon"]}
    for d in DEPOTS.values()
]
_seen = set()
WEATHER_STATIONS = [s for s in WEATHER_STATIONS if s["name"] not in _seen and not _seen.add(s["name"])]

# ── Public Holidays ──────────────────────────────────────────────────────────

EASTER_DATES = {date(2026, 4, 3), date(2026, 4, 4), date(2026, 4, 5), date(2026, 4, 6)}
PUBLIC_HOLIDAYS = EASTER_DATES | {
    date(2026, 5, 4),  # Labour Day (QLD)
}
HOLIDAY_NOTE = "Easter 2026 is 3-6 April (Good Friday to Easter Monday) — no planned work over Easter."
