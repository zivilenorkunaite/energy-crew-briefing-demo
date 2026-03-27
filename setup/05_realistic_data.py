"""Generate realistic operations data for Energy Crew Briefing demo.

Creates depot-based crews with real Australian names, historical work orders
for March 2026 and scheduled work through mid-April 2026.
Easter 2026: Good Friday 3 Apr, Easter Saturday 4 Apr, Easter Monday 6 Apr.

Run with: python3 setup/05_realistic_data.py
"""

import json
import random
import sys
import os
from datetime import date, timedelta, datetime

sys.path.insert(0, os.path.dirname(__file__))
from helpers import run_sql, UC_FULL

# ── Easter 2026 public holidays ──
EASTER_DATES = {date(2026, 4, 3), date(2026, 4, 4), date(2026, 4, 5), date(2026, 4, 6)}
# Other public holidays in range
PUBLIC_HOLIDAYS = EASTER_DATES | {
    date(2026, 3, 2),   # Bank Holiday (regional)
}

random.seed(42)  # reproducible

# ── Realistic depot-based crews (Queensland) ──
CREWS = {
    "Townsville Lines A": {
        "depot": "Townsville", "type": "Planned Maintenance",
        "members": [
            {"name": "Greg Thompson", "role": "Crew Leader"},
            {"name": "Shane Murray", "role": "Powerline Worker"},
            {"name": "Ben O'Brien", "role": "Powerline Worker"},
            {"name": "Lachlan Mitchell", "role": "Apprentice"},
        ],
    },
    "Townsville Lines B": {
        "depot": "Townsville", "type": "Corrective Maintenance",
        "members": [
            {"name": "Darren Walsh", "role": "Crew Leader"},
            {"name": "Matt Sullivan", "role": "Powerline Worker"},
            {"name": "Cooper Ryan", "role": "Powerline Worker"},
        ],
    },
    # Depot: Cairns (North Queensland)
    "Cairns Lines": {
        "depot": "Cairns", "type": "Planned Maintenance",
        "members": [
            {"name": "Craig Williams", "role": "Crew Leader"},
            {"name": "Jason Kelly", "role": "Powerline Worker"},
            {"name": "Riley Brown", "role": "Powerline Worker"},
            {"name": "Jack Anderson", "role": "Apprentice"},
        ],
    },
    "Cairns Cable": {
        "depot": "Cairns", "type": "Capital Works",
        "members": [
            {"name": "Adam Stewart", "role": "Senior Cable Jointer"},
            {"name": "Nathan Chen", "role": "Cable Jointer"},
        ],
    },
    # Depot: Innisfail (North Queensland)
    "Innisfail Lines": {
        "depot": "Innisfail", "type": "Corrective Maintenance",
        "members": [
            {"name": "Wayne Campbell", "role": "Crew Leader"},
            {"name": "Josh Henderson", "role": "Powerline Worker"},
            {"name": "Ethan Taylor", "role": "Powerline Worker"},
        ],
    },
    # Depot: Mackay (North Queensland)
    "Mackay Lines": {
        "depot": "Mackay", "type": "Planned Maintenance",
        "members": [
            {"name": "Brett Robertson", "role": "Crew Leader"},
            {"name": "Chris Johnson", "role": "Powerline Worker"},
            {"name": "Thomas Wilson", "role": "Powerline Worker"},
            {"name": "Bailey Smith", "role": "Apprentice"},
        ],
    },
    # Depot: Rockhampton (Northern)
    "Rockhampton Lines": {
        "depot": "Rockhampton", "type": "Planned Maintenance",
        "members": [
            {"name": "Mick Anderson", "role": "Crew Leader"},
            {"name": "Luke Campbell", "role": "Powerline Worker"},
            {"name": "Ryan Singh", "role": "Powerline Worker"},
        ],
    },
    "Rockhampton Substation": {
        "depot": "Rockhampton", "type": "Capital Works",
        "members": [
            {"name": "Andrew McPherson", "role": "Senior Electrical Tech"},
            {"name": "Noah Williams", "role": "Electrical Tech"},
        ],
    },
    # Depot: Bundaberg (Northern Tablelands)
    "Bundaberg Lines": {
        "depot": "Bundaberg", "type": "Planned Maintenance",
        "members": [
            {"name": "Col Stewart", "role": "Crew Leader"},
            {"name": "Liam Brown", "role": "Powerline Worker"},
            {"name": "Will Thompson", "role": "Powerline Worker"},
        ],
    },
    "Bundaberg Inspection": {
        "depot": "Bundaberg", "type": "Inspection",
        "members": [
            {"name": "Sarah Mitchell", "role": "Senior Inspector"},
            {"name": "Emma Walsh", "role": "Asset Inspector"},
            {"name": "Brooke Nguyen", "role": "Asset Inspector"},
        ],
    },
    # Depot: Toowoomba (Western/Ranges)
    "Toowoomba Lines": {
        "depot": "Toowoomba", "type": "Planned Maintenance",
        "members": [
            {"name": "Gary Wilson", "role": "Crew Leader"},
            {"name": "Kevin Murray", "role": "Powerline Worker"},
            {"name": "Josh Patel", "role": "Powerline Worker"},
            {"name": "Jack Sullivan", "role": "Apprentice"},
        ],
    },
    "Toowoomba Cable": {
        "depot": "Toowoomba", "type": "Capital Works",
        "members": [
            {"name": "Nicole Henderson", "role": "Senior Cable Jointer"},
            {"name": "Matt Robertson", "role": "Cable Jointer"},
        ],
    },
    # Depot: Roma (Western/Macquarie)
    "Roma Lines": {
        "depot": "Roma", "type": "Planned Maintenance",
        "members": [
            {"name": "Shane Taylor", "role": "Crew Leader"},
            {"name": "Ben Williams", "role": "Powerline Worker"},
            {"name": "Cooper Johnson", "role": "Powerline Worker"},
        ],
    },
    "Roma Emergency": {
        "depot": "Roma", "type": "Emergency Response",
        "members": [
            {"name": "Darren Brown", "role": "Crew Leader"},
            {"name": "Ryan Kelly", "role": "Powerline Worker"},
        ],
    },
    # Depot: Emerald (Ranges)
    "Emerald Lines": {
        "depot": "Emerald", "type": "Planned Maintenance",
        "members": [
            {"name": "Craig Anderson", "role": "Crew Leader"},
            {"name": "Jason Thompson", "role": "Powerline Worker"},
            {"name": "Riley Mitchell", "role": "Apprentice"},
        ],
    },
    # Depot: Mount Isa (North Queensland Queensland)
    "Mount Isa Lines": {
        "depot": "Mount Isa", "type": "Planned Maintenance",
        "members": [
            {"name": "Adam Walsh", "role": "Crew Leader"},
            {"name": "Nathan Murray", "role": "Powerline Worker"},
            {"name": "Ethan Stewart", "role": "Powerline Worker"},
            {"name": "Lachlan Wilson", "role": "Apprentice"},
        ],
    },
    "Mount Isa Inspection": {
        "depot": "Mount Isa", "type": "Inspection",
        "members": [
            {"name": "Kylie Robertson", "role": "Senior Inspector"},
            {"name": "Tegan Campbell", "role": "Asset Inspector"},
        ],
    },
    # Depot: Longreach (Central West Queensland)
    "Longreach Lines": {
        "depot": "Longreach", "type": "Planned Maintenance",
        "members": [
            {"name": "Mick O'Brien", "role": "Crew Leader"},
            {"name": "Luke Henderson", "role": "Powerline Worker"},
        ],
    },
    # Depot: Charters Towers (North Queensland)
    "Charters Towers Lines": {
        "depot": "Charters Towers", "type": "Planned Maintenance",
        "members": [
            {"name": "Brett Singh", "role": "Crew Leader"},
            {"name": "Chris Sullivan", "role": "Powerline Worker"},
        ],
    },
    # Depot: Atherton (Tablelands)
    "Atherton Lines": {
        "depot": "Atherton", "type": "Corrective Maintenance",
        "members": [
            {"name": "Wayne Anderson", "role": "Crew Leader"},
            {"name": "Josh Murray", "role": "Powerline Worker"},
        ],
    },
    # Depot: Gympie (Wide Bay)
    "Gympie Lines": {
        "depot": "Gympie", "type": "Planned Maintenance",
        "members": [
            {"name": "Kevin Brown", "role": "Crew Leader"},
            {"name": "Thomas Kelly", "role": "Powerline Worker"},
        ],
    },
    # Depot: Gladstone (Central Queensland)
    "Gladstone Lines": {
        "depot": "Gladstone", "type": "Planned Maintenance",
        "members": [
            {"name": "Gary Thompson", "role": "Crew Leader"},
            {"name": "Matt Campbell", "role": "Powerline Worker"},
            {"name": "Noah Mitchell", "role": "Apprentice"},
        ],
    },
    # Contractors
    "Contractor Downer": {
        "depot": "Various", "type": "Vegetation Management",
        "members": [
            {"name": "Steve Downer-TL", "role": "Tree Crew Leader"},
            {"name": "Jake Downer-A", "role": "Arborist"},
            {"name": "Mark Downer-B", "role": "Tree Trimmer"},
        ],
    },
    "Contractor Asplundh": {
        "depot": "Various", "type": "Vegetation Management",
        "members": [
            {"name": "Brad Asplundh-TL", "role": "Tree Crew Leader"},
            {"name": "Sam Asplundh-A", "role": "Arborist"},
        ],
    },
    "Contractor Fulton Hogan": {
        "depot": "Various", "type": "Capital Works",
        "members": [
            {"name": "Dave FH-PM", "role": "Project Manager"},
            {"name": "Tony FH-Eng", "role": "Civil Engineer"},
            {"name": "James FH-Sup", "role": "Site Supervisor"},
        ],
    },
}

# ── Work order templates by type ──
WO_TEMPLATES = {
    "Planned Maintenance": [
        ("Pole inspection and treatment — {loc}", "Inspect timber poles for decay, treat with preservative. Check stays and cross-arms."),
        ("Conductor re-tension — {loc} feeder", "Re-tension sagging conductor on 11kV feeder. Check clearances to ground and structures."),
        ("Cross-arm replacement — {loc}", "Replace deteriorated hardwood cross-arms with steel. De-energise and isolate before work."),
        ("Insulator replacement — {loc} HV line", "Replace cracked or tracking porcelain insulators on 66kV transmission line."),
        ("Transformer oil sampling — {loc} zone sub", "Collect oil samples from power transformers for dissolved gas analysis."),
        ("Earthing system test — {loc}", "Test earth resistance at substation and along feeder. Repair defective earth connections."),
        ("Recloser maintenance — {loc} feeder", "Service auto-recloser. Check trip settings, oil level, and battery backup."),
        ("SWER line maintenance — {loc}", "Single Wire Earth Return line inspection and maintenance. Check isolating transformers."),
        ("Fuse coordination review — {loc}", "Review and adjust fuse ratings on distribution network. Replace non-standard fuses."),
        ("Street light repair — {loc} CBD", "Repair faulty street lights reported by council. Replace PE cells and lamps."),
    ],
    "Corrective Maintenance": [
        ("Storm damage repair — {loc}", "Repair lines damaged by recent storm event. Replace broken poles and conductor."),
        ("Faulted cable repair — {loc}", "Locate and repair underground cable fault. Excavate, joint, and backfill."),
        ("Leaking transformer — {loc}", "Investigate and repair oil leak on pole-mount transformer. Check oil level and gaskets."),
        ("Broken cross-arm — {loc}", "Emergency replacement of broken cross-arm. Lines currently de-energised."),
        ("Fallen conductor — {loc}", "Respond to fallen power line report. Make safe, replace conductor section."),
        ("Vehicle impact damage — {loc}", "Repair pole and equipment damaged by vehicle collision. Coordinate with police."),
        ("Bird/animal strike — {loc}", "Repair equipment damaged by wildlife contact. Install bird diverters."),
        ("Bushfire damage assessment — {loc}", "Assess and repair power infrastructure damaged in recent bushfire."),
    ],
    "Capital Works": [
        ("New subdivision connection — {loc}", "Install underground cable network for new residential subdivision. 30 lots."),
        ("Feeder upgrade — {loc} 11kV", "Upgrade conductor from 7/.064 to Moon on 11kV feeder to increase capacity."),
        ("Zone substation transformer replacement — {loc}", "Replace aging 10MVA transformer with new 20MVA unit at zone substation."),
        ("Underground conversion — {loc} main street", "Convert overhead lines to underground as part of council streetscape project."),
        ("Solar farm connection — {loc}", "Install new 33kV connection point for 5MW solar farm development."),
        ("Battery storage installation — {loc}", "Install community battery storage system (2MWh) for network support."),
    ],
    "Inspection": [
        ("Pole condition assessment — {loc} area", "Systematic pole inspection program. Visual, hammer test, and drilling where required."),
        ("Drone inspection — {loc} 66kV transmission", "Aerial drone inspection of 66kV transmission line. Thermal and visual imaging."),
        ("Vegetation clearance audit — {loc}", "Audit vegetation clearances on feeders. Flag non-compliant spans for trimming."),
        ("Stay wire inspection — {loc}", "Inspect all stay wires and anchors in area. Replace corroded or damaged stays."),
        ("Switchgear inspection — {loc} zone sub", "Detailed inspection of 11kV and 33kV switchgear at zone substation."),
    ],
    "Emergency Response": [
        ("Storm response — {loc} region", "Multiple outages from severe weather. Priority restoration of critical loads."),
        ("Bushfire standby — {loc}", "Pre-position crews for potential bushfire impact on network infrastructure."),
        ("Flood response — {loc}", "Assess and restore supply to flood-affected areas. De-energise submerged equipment."),
        ("Vehicle vs pole — {loc}", "Emergency response to vehicle collision with power pole. Make safe and restore."),
    ],
    "Vegetation Management": [
        ("Powerline clearance trim — {loc} feeder", "Routine vegetation trimming to maintain clearances on distribution feeder."),
        ("Hazard tree removal — {loc}", "Remove trees assessed as high risk of falling onto powerlines."),
        ("Bushfire preparedness — {loc} area", "Pre-summer vegetation clearance in bushfire-prone zones."),
        ("Emergency tree removal — {loc}", "Remove tree that has fallen onto or near powerlines."),
    ],
    "Asset Replacement": [
        ("Pole replacement program — {loc}", "Replace condemned poles identified in condition assessment. 8 poles this batch."),
        ("Transformer replacement — {loc}", "Replace end-of-life pole-mount transformer with new unit."),
        ("Switchgear upgrade — {loc} zone sub", "Replace obsolete oil circuit breaker with modern vacuum switchgear."),
        ("Meter board upgrade — {loc}", "Replace aging meter boards at customer premises to meet current standards."),
    ],
}

PRIORITIES = ["Critical", "High", "Medium", "Low"]
PRIORITY_WEIGHTS = [5, 20, 50, 25]

# ── Asset types (realistic Australian energy distribution) ──
ASSET_TYPES = [
    {"id": 1, "name": "Timber Pole", "category": "Poles & Structures", "lifespan": 45, "inspection_months": 60},
    {"id": 2, "name": "Concrete Pole", "category": "Poles & Structures", "lifespan": 60, "inspection_months": 60},
    {"id": 3, "name": "Steel Pole", "category": "Poles & Structures", "lifespan": 50, "inspection_months": 60},
    {"id": 4, "name": "Stobie Pole", "category": "Poles & Structures", "lifespan": 70, "inspection_months": 60},
    {"id": 5, "name": "Stay Wire", "category": "Poles & Structures", "lifespan": 30, "inspection_months": 60},
    {"id": 6, "name": "Overhead Conductor", "category": "Conductors & Cables", "lifespan": 50, "inspection_months": 120},
    {"id": 7, "name": "Underground Cable", "category": "Conductors & Cables", "lifespan": 40, "inspection_months": 120},
    {"id": 8, "name": "Service Line", "category": "Conductors & Cables", "lifespan": 35, "inspection_months": 120},
    {"id": 9, "name": "Aerial Bundled Cable", "category": "Conductors & Cables", "lifespan": 35, "inspection_months": 120},
    {"id": 10, "name": "Pole-Mount Transformer", "category": "Transformers", "lifespan": 35, "inspection_months": 24},
    {"id": 11, "name": "Pad-Mount Transformer", "category": "Transformers", "lifespan": 35, "inspection_months": 24},
    {"id": 12, "name": "Zone Substation Transformer", "category": "Transformers", "lifespan": 40, "inspection_months": 12},
    {"id": 13, "name": "Recloser", "category": "Switchgear", "lifespan": 25, "inspection_months": 12},
    {"id": 14, "name": "Sectionaliser", "category": "Switchgear", "lifespan": 25, "inspection_months": 12},
    {"id": 15, "name": "Fuse Switch", "category": "Switchgear", "lifespan": 30, "inspection_months": 24},
    {"id": 16, "name": "Ring Main Unit", "category": "Switchgear", "lifespan": 30, "inspection_months": 24},
    {"id": 17, "name": "Circuit Breaker", "category": "Switchgear", "lifespan": 30, "inspection_months": 12},
    {"id": 18, "name": "Surge Arrester", "category": "Protection", "lifespan": 20, "inspection_months": 60},
    {"id": 19, "name": "Insulator", "category": "Protection", "lifespan": 40, "inspection_months": 60},
    {"id": 20, "name": "Earthing System", "category": "Protection", "lifespan": 25, "inspection_months": 24},
    {"id": 21, "name": "Hardwood Cross-Arm", "category": "Cross-Arms & Hardware", "lifespan": 25, "inspection_months": 60},
    {"id": 22, "name": "Steel Cross-Arm", "category": "Cross-Arms & Hardware", "lifespan": 40, "inspection_months": 60},
    {"id": 23, "name": "Fibreglass Cross-Arm", "category": "Cross-Arms & Hardware", "lifespan": 35, "inspection_months": 60},
    {"id": 24, "name": "Street Light", "category": "Other", "lifespan": 20, "inspection_months": 24},
    {"id": 25, "name": "Smart Meter", "category": "Metering", "lifespan": 15, "inspection_months": 60},
    {"id": 26, "name": "Pillar Box", "category": "Other", "lifespan": 30, "inspection_months": 24},
]

# Map work order types to likely asset types
WO_TYPE_TO_ASSET_TYPES = {
    "Planned Maintenance": [1, 2, 3, 5, 6, 10, 13, 14, 20, 21, 22],
    "Corrective Maintenance": [1, 2, 6, 7, 10, 13, 21, 22],
    "Capital Works": [7, 10, 11, 12, 16, 17],
    "Inspection": [1, 2, 3, 5, 6, 19, 21, 22],
    "Emergency Response": [1, 2, 6, 10, 13],
    "Vegetation Management": [1, 2, 6, 21, 22],
    "Asset Replacement": [1, 2, 10, 13, 15, 17, 21, 22, 23],
}

VOLTAGE_LEVELS = ["LV (415V)", "11kV", "22kV", "33kV", "66kV"]
VOLTAGE_WEIGHTS = [20, 50, 10, 15, 5]
CONDITIONS = ["Good", "Fair", "Poor", "Critical"]
CONDITION_WEIGHTS = [35, 40, 20, 5]

STREET_NAMES = [
    "Prince St", "Fitzroy St", "Clarence St", "Victoria St", "King St",
    "Railway Parade", "Pacific Hwy", "Main Rd", "Church St", "Bridge St",
    "River Rd", "Station St", "High St", "George St", "Smith St",
    "Bent St", "Miller St", "Queen St", "Park Ave", "Hill St",
]

# Locations per depot for work order titles
DEPOT_LOCATIONS = {
    "Townsville": ["Townsville", "Aitkenvale", "Kirwan", "Thuringowa", "Magnetic Island", "Ingham"],
    "Cairns": ["Cairns", "Smithfield", "Edmonton", "Gordonvale", "Mareeba", "Port Douglas"],
    "Mackay": ["Mackay", "Sarina", "Walkerston", "Marian", "Mirani", "Proserpine"],
    "Rockhampton": ["Rockhampton", "Yeppoon", "Gracemere", "Mount Morgan", "Emu Park"],
    "Bundaberg": ["Bundaberg", "Bargara", "Childers", "Gin Gin", "Burnett Heads"],
    "Gladstone": ["Gladstone", "Tannum Sands", "Calliope", "Boyne Island"],
    "Toowoomba": ["Toowoomba", "Highfields", "Oakey", "Pittsworth", "Dalby"],
    "Roma": ["Roma", "Mitchell", "Surat", "St George", "Charleville"],
    "Emerald": ["Emerald", "Blackwater", "Springsure", "Clermont", "Capella"],
    "Mount Isa": ["Mount Isa", "Cloncurry", "Julia Creek", "Camooweal"],
    "Longreach": ["Longreach", "Barcaldine", "Blackall", "Winton"],
    "Innisfail": ["Innisfail", "Tully", "Mission Beach", "Babinda", "Mourilyan"],
    "Charters Towers": ["Charters Towers", "Mingela", "Ravenswood", "Pentland"],
    "Atherton": ["Atherton", "Malanda", "Herberton", "Ravenshoe", "Yungaburra"],
    "Gympie": ["Gympie", "Tin Can Bay", "Rainbow Beach", "Kilkivan"],
    "Various": ["Townsville", "Cairns", "Mackay", "Rockhampton", "Bundaberg", "Toowoomba"],
}


def is_workday(d: date) -> bool:
    return d.weekday() < 5 and d not in EASTER_DATES and d not in PUBLIC_HOLIDAYS


def create_tables():
    """Create work_orders and work_tasks tables if they don't exist."""
    print("\n=== Creating tables ===")

    run_sql(f"CREATE SCHEMA IF NOT EXISTS {UC_FULL}")

    run_sql(f"""
        CREATE TABLE IF NOT EXISTS {UC_FULL}.work_orders (
            id INT,
            wo_number STRING,
            project_id INT,
            asset_id INT,
            title STRING,
            wo_type STRING,
            priority STRING,
            status STRING,
            created_date DATE,
            scheduled_date DATE,
            completed_date DATE,
            estimated_hours DOUBLE,
            actual_hours DOUBLE,
            assigned_crew STRING,
            location STRING,
            description STRING
        )
    """)
    # Schema migration: add location column if upgrading from older version
    run_sql(f"ALTER TABLE {UC_FULL}.work_orders ADD COLUMN IF NOT EXISTS location STRING")
    print("  work_orders table ready")

    run_sql(f"""
        CREATE TABLE IF NOT EXISTS {UC_FULL}.work_tasks (
            id INT,
            work_order_id INT,
            task_number STRING,
            sequence INT,
            task_name STRING,
            status STRING,
            scheduled_datetime STRING,
            completed_datetime STRING,
            assigned_to STRING,
            estimated_hours DOUBLE,
            actual_hours DOUBLE,
            notes STRING
        )
    """)
    print("  work_tasks table ready")

    run_sql(f"""
        CREATE TABLE IF NOT EXISTS {UC_FULL}.asset_types (
            id INT,
            name STRING,
            category STRING,
            typical_lifespan_years INT,
            inspection_interval_months INT
        )
    """)
    print("  asset_types table ready")

    run_sql(f"""
        CREATE TABLE IF NOT EXISTS {UC_FULL}.assets (
            id INT,
            asset_number STRING,
            asset_type STRING,
            asset_category STRING,
            location STRING,
            address STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            install_date DATE,
            condition_rating STRING,
            last_inspection_date DATE,
            next_inspection_due DATE,
            voltage_level STRING,
            image_path STRING
        )
    """)
    print("  assets table ready")


def generate_asset_types():
    """Seed the asset_types lookup table."""
    print("\n=== Seeding asset_types ===")
    run_sql(f"DELETE FROM {UC_FULL}.asset_types WHERE id > 0")
    values = []
    for at in ASSET_TYPES:
        values.append(f"({at['id']}, '{at['name']}', '{at['category']}', {at['lifespan']}, {at['inspection_months']})")
    run_sql(f"INSERT INTO {UC_FULL}.asset_types VALUES {', '.join(values)}")
    print(f"  {len(ASSET_TYPES)} asset types seeded")


def generate_assets():
    """Generate ~500 realistic network assets distributed across depots."""
    print("\n=== Generating assets ===")
    run_sql(f"DELETE FROM {UC_FULL}.assets WHERE id > 0")

    all_assets = []
    asset_id = 1

    for depot, locations in DEPOT_LOCATIONS.items():
        # Each depot gets 25-40 assets
        n_assets = random.randint(25, 40)
        for _ in range(n_assets):
            at = random.choice(ASSET_TYPES)
            loc = random.choice(locations)
            street = random.choice(STREET_NAMES)
            street_num = random.randint(1, 350)

            # Get lat/lon from customise depots (approximate — add random offset)
            depot_lower = depot.lower()
            base_lat, base_lon = -30.0, 150.0  # fallback
            for k, v in DEPOT_LOCATIONS.items():
                if k == depot:
                    # Use the depot location from customise
                    from server.customise import DEPOTS as BRAND_DEPOTS
                    dk = depot.lower()
                    if dk in BRAND_DEPOTS:
                        base_lat = BRAND_DEPOTS[dk]["lat"]
                        base_lon = BRAND_DEPOTS[dk]["lon"]
                    break
            lat = round(base_lat + random.uniform(-0.1, 0.1), 4)
            lon = round(base_lon + random.uniform(-0.1, 0.1), 4)

            lifespan = at["lifespan"]
            age = random.randint(1, lifespan)
            install_year = 2026 - age
            install_date = date(install_year, random.randint(1, 12), random.randint(1, 28))

            condition = random.choices(CONDITIONS, weights=CONDITION_WEIGHTS)[0]
            # Older assets more likely to be in poor condition
            if age > lifespan * 0.8:
                condition = random.choices(CONDITIONS, weights=[10, 30, 40, 20])[0]
            elif age > lifespan * 0.6:
                condition = random.choices(CONDITIONS, weights=[20, 40, 30, 10])[0]

            insp_months = at["inspection_months"]
            last_insp = date(2025, random.randint(1, 12), random.randint(1, 28))
            next_insp = date(last_insp.year, last_insp.month, last_insp.day) + timedelta(days=insp_months * 30)

            voltage = random.choices(VOLTAGE_LEVELS, weights=VOLTAGE_WEIGHTS)[0]

            # Asset number prefix based on type
            type_prefix = at["name"][:2].upper()
            asset_number = f"{type_prefix}{depot[:2].upper()}-{asset_id:05d}"

            all_assets.append({
                "id": asset_id,
                "asset_number": asset_number,
                "asset_type": at["name"],
                "asset_category": at["category"],
                "location": loc,
                "address": f"{street_num} {street}, {loc}",
                "latitude": lat,
                "longitude": lon,
                "install_date": install_date.isoformat(),
                "condition_rating": condition,
                "last_inspection_date": last_insp.isoformat(),
                "next_inspection_due": next_insp.isoformat(),
                "voltage_level": voltage,
                "image_path": f"/Volumes/{UC_FULL.replace('.', '/')}/asset_images/{at['name'].lower().replace(' ', '_')}.png",
            })
            asset_id += 1

    # Insert in batches
    batch_size = 50
    for i in range(0, len(all_assets), batch_size):
        batch = all_assets[i:i+batch_size]
        values = []
        for a in batch:
            addr = a['address'].replace("'", "''")
            values.append(
                f"({a['id']}, '{a['asset_number']}', '{a['asset_type']}', '{a['asset_category']}', "
                f"'{a['location']}', '{addr}', {a['latitude']}, {a['longitude']}, "
                f"'{a['install_date']}', '{a['condition_rating']}', '{a['last_inspection_date']}', "
                f"'{a['next_inspection_due']}', '{a['voltage_level']}', '{a['image_path']}')"
            )
        run_sql(f"INSERT INTO {UC_FULL}.assets VALUES {', '.join(values)}")
        print(f"  Batch {i//batch_size + 1}: {len(batch)} assets")

    print(f"  Total: {len(all_assets)} assets across {len(DEPOT_LOCATIONS)} depots")

    # Build index for WO → asset linking
    global _ASSET_INDEX
    _ASSET_INDEX = {}
    for a in all_assets:
        key = a["location"]
        _ASSET_INDEX.setdefault(key, []).append(a["id"])
    # Also index by depot name
    for depot, locs in DEPOT_LOCATIONS.items():
        depot_ids = []
        for loc in locs:
            depot_ids.extend(_ASSET_INDEX.get(loc, []))
        _ASSET_INDEX[depot] = depot_ids

    return all_assets


_ASSET_INDEX: dict[str, list[int]] = {}


def _pick_asset_id(wo_type: str, depot: str) -> int:
    """Pick a random asset ID from the depot, preferring matching type."""
    depot_assets = _ASSET_INDEX.get(depot, [])
    if depot_assets:
        return random.choice(depot_assets)
    # Fallback to any asset
    all_ids = [aid for ids in _ASSET_INDEX.values() for aid in ids]
    return random.choice(all_ids) if all_ids else 1


def generate_asset_images():
    """Generate placeholder PNG images for each asset type using Pillow."""
    print("\n=== Generating asset images ===")
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        print("  Pillow not installed — skipping image generation")
        print("  Install with: pip install Pillow")
        return

    import os
    img_dir = os.path.join(os.path.dirname(__file__), "..", "asset_images")
    os.makedirs(img_dir, exist_ok=True)

    # Color map per category
    category_colors = {
        "Poles & Structures": "#2E86AB",
        "Conductors & Cables": "#A23B72",
        "Transformers": "#F18F01",
        "Switchgear": "#C73E1D",
        "Protection": "#3B1F2B",
        "Metering": "#44BBA4",
        "Cross-Arms & Hardware": "#8B6914",
        "Other": "#6C757D",
    }

    for at in ASSET_TYPES:
        name = at["name"]
        category = at["category"]
        color = category_colors.get(category, "#6C757D")

        # Create a simple labeled placeholder image
        img = Image.new("RGB", (400, 300), color)
        draw = ImageDraw.Draw(img)

        # White text area
        draw.rectangle([20, 20, 380, 280], fill="#FFFFFF", outline=color, width=2)

        # Asset type name
        try:
            font_large = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 24)
            font_small = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 14)
        except (OSError, IOError):
            font_large = ImageFont.load_default()
            font_small = ImageFont.load_default()

        draw.text((30, 40), name, fill=color, font=font_large)
        draw.text((30, 80), f"Category: {category}", fill="#666666", font=font_small)
        draw.text((30, 105), f"Lifespan: {at['lifespan']} years", fill="#666666", font=font_small)
        draw.text((30, 130), f"Inspection: every {at['inspection_months']} months", fill="#666666", font=font_small)

        # Placeholder icon area
        draw.rectangle([30, 160, 370, 270], fill="#F8F9FA", outline="#DEE2E6")
        draw.text((140, 205), "[Asset Photo]", fill="#ADB5BD", font=font_small)

        filename = f"{name.lower().replace(' ', '_')}.png"
        img.save(os.path.join(img_dir, filename))

    print(f"  Generated {len(ASSET_TYPES)} placeholder images in {img_dir}/")

    # Create UC Volume and upload images
    print("  Creating UC Volume for asset images...")
    run_sql(f"CREATE VOLUME IF NOT EXISTS {UC_FULL}.asset_images")

    print("  Uploading images to UC Volume...")
    from helpers import get_host, PROFILE
    import urllib.request

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(profile=PROFILE)
        host = w.config.host
        token = w.config.token or w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
    except Exception as e:
        print(f"  Could not get auth for upload: {e}")
        return

    uploaded = 0
    for filename in os.listdir(img_dir):
        if not filename.endswith(".png"):
            continue
        filepath = os.path.join(img_dir, filename)
        volume_path = f"/Volumes/{UC_FULL.replace('.', '/')}/asset_images/{filename}"
        url = f"{host}/api/2.0/fs/files{volume_path}?overwrite=true"

        with open(filepath, "rb") as f:
            data = f.read()

        req = urllib.request.Request(
            url, data=data, method="PUT",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"},
        )
        try:
            urllib.request.urlopen(req, timeout=15)
            uploaded += 1
        except Exception as e:
            print(f"  Upload failed for {filename}: {e}")

    print(f"  Uploaded {uploaded}/{len(ASSET_TYPES)} images to UC Volume")


def generate_work_orders():
    """Generate work orders: 30 days history + 20 days forward from today."""
    print("\n=== Generating work orders ===")

    today = date.today()
    start_date = today - timedelta(days=30)
    end_date = today + timedelta(days=20)
    print(f"  Date range: {start_date} to {end_date} (today: {today})")

    wo_id = 10000  # Start after existing data
    task_id = 50000
    all_wos = []
    all_tasks = []

    for crew_name, crew_info in CREWS.items():
        depot = crew_info["depot"]
        primary_type = crew_info["type"]
        members = crew_info["members"]
        locations = DEPOT_LOCATIONS.get(depot, [depot])

        # Generate ~2-3 work orders per workday per crew
        d = start_date
        while d <= end_date:
            if not is_workday(d):
                d += timedelta(days=1)
                continue

            # Number of WOs depends on crew size and type
            n_wos = random.choices([1, 2, 3], weights=[20, 50, 30])[0]
            if "Emergency" in primary_type:
                n_wos = random.choices([0, 1, 2], weights=[40, 40, 20])[0]
            if "Contractor" in crew_name:
                n_wos = random.choices([1, 2], weights=[40, 60])[0]

            for _ in range(n_wos):
                # Pick work type — mostly primary but sometimes others
                if random.random() < 0.75:
                    wo_type = primary_type
                else:
                    wo_type = random.choice(list(WO_TEMPLATES.keys()))

                templates = WO_TEMPLATES[wo_type]
                title_tpl, desc_tpl = random.choice(templates)
                loc = random.choice(locations)
                title = title_tpl.format(loc=loc)
                desc = desc_tpl

                priority = random.choices(PRIORITIES, weights=PRIORITY_WEIGHTS)[0]

                # Status based on date relative to "today"
                if d < today - timedelta(days=7):
                    status = random.choices(
                        ["Completed", "Completed", "Completed", "Cancelled"],
                        weights=[70, 15, 10, 5],
                    )[0]
                elif d < today:
                    status = random.choices(
                        ["Completed", "In Progress", "Open"],
                        weights=[60, 25, 15],
                    )[0]
                elif d == today:
                    status = random.choices(
                        ["In Progress", "Open", "Completed"],
                        weights=[50, 40, 10],
                    )[0]
                else:
                    status = "Open"

                est_hours = round(random.uniform(2, 12), 1)
                if status == "Completed":
                    actual_hours = round(est_hours * random.uniform(0.7, 1.4), 1)
                    completed = d + timedelta(days=random.randint(0, 1))
                elif status == "In Progress":
                    actual_hours = round(est_hours * random.uniform(0.2, 0.7), 1)
                    completed = None
                else:
                    actual_hours = None
                    completed = None

                wo_number = f"WO-{today.year}-{wo_id:05d}"
                created = d - timedelta(days=random.randint(1, 14))

                all_wos.append({
                    "id": wo_id,
                    "wo_number": wo_number,
                    "project_id": random.randint(1, 120),
                    "asset_id": _pick_asset_id(wo_type, depot),
                    "title": title.replace("'", "''"),
                    "wo_type": wo_type,
                    "priority": priority,
                    "status": status,
                    "created_date": created.isoformat(),
                    "scheduled_date": d.isoformat(),
                    "completed_date": completed.isoformat() if completed else None,
                    "estimated_hours": est_hours,
                    "actual_hours": actual_hours,
                    "assigned_crew": crew_name,
                    "location": loc,
                    "description": desc.replace("'", "''"),
                })

                # Generate tasks with proper sequencing
                # Ordered task template — sequence matters!
                ordered_tasks = [
                    "Material preparation and staging",
                    "Site assessment and safety briefing",
                    "Isolate and test for dead",
                    "Set up worksite and barriers",
                    f"Perform {wo_type.lower()} work",
                    "Test and commission",
                    "Restore supply and remove isolation",
                    "Quality inspection",
                    "Clean up and demobilise",
                    "Complete paperwork and close out",
                ]
                n_tasks = random.randint(3, 6)
                # Always include first 2 + core work + last 2, fill middle randomly
                must_have = [0, 1, 4, -2, -1]  # indices
                selected_indices = sorted(set(
                    [i % len(ordered_tasks) for i in must_have] +
                    random.sample(range(len(ordered_tasks)), min(n_tasks, len(ordered_tasks)))
                ))
                selected_tasks = [ordered_tasks[i] for i in selected_indices[:n_tasks]]

                for seq, task_name in enumerate(selected_tasks, 1):
                    assigned = random.choice(members)

                    # Task status depends on WO status and date
                    if status in ("Completed", "Cancelled"):
                        task_status = status
                    elif d > today:
                        # Future WO — all tasks must be Open
                        task_status = "Open"
                    elif d == today:
                        # Today — earlier tasks may be done, later ones open
                        if seq <= 2 and status == "In Progress":
                            task_status = random.choices(["Completed", "In Progress"], weights=[70, 30])[0]
                        elif seq <= 2:
                            task_status = "Open"
                        else:
                            task_status = "Open"
                    else:
                        # Past — sequential completion (earlier tasks more likely done)
                        if status == "In Progress":
                            # Some tasks done, some in progress
                            completion_prob = max(0, 80 - seq * 15)
                            task_status = random.choices(
                                ["Completed", "In Progress", "Open"],
                                weights=[completion_prob, 15, max(5, 100 - completion_prob - 15)]
                            )[0]
                        else:
                            task_status = "Open"

                    t_est = round(est_hours / n_tasks * random.uniform(0.5, 1.5), 1)
                    t_actual = round(t_est * random.uniform(0.8, 1.3), 1) if task_status == "Completed" else None

                    all_tasks.append({
                        "id": task_id,
                        "work_order_id": wo_id,
                        "task_number": f"T-{task_id:06d}",
                        "sequence": seq,
                        "task_name": task_name.replace("'", "''"),
                        "status": task_status,
                        "scheduled_datetime": d.isoformat(),
                        "completed_datetime": (d + timedelta(days=random.randint(0, 1))).isoformat() if task_status == "Completed" else None,
                        "assigned_to": assigned["name"].replace("'", "''"),
                        "estimated_hours": t_est,
                        "actual_hours": t_actual,
                        "notes": f"{assigned['role']}. {crew_name} depot: {depot}." if random.random() < 0.3 else None,
                    })
                    task_id += 1

                wo_id += 1
            d += timedelta(days=1)

    # ── Ensure ALL crews have work orders on every weekday for the next 20 days ──
    all_crew_names = [n for n in CREWS.keys() if CREWS[n]["depot"] != "Various"]

    # Build index: crew -> set of dates with WOs
    crew_dates = {cn: set() for cn in all_crew_names}
    for wo in all_wos:
        cn = wo["assigned_crew"]
        if cn in crew_dates:
            try:
                sd = date.fromisoformat(wo["scheduled_date"]) if isinstance(wo["scheduled_date"], str) else wo["scheduled_date"]
                crew_dates[cn].add(sd)
            except (ValueError, TypeError):
                pass

    # Collect all future weekdays
    future_weekdays = []
    for day_offset in range(1, 21):
        d = today + timedelta(days=day_offset)
        if d.weekday() < 5 and d not in PUBLIC_HOLIDAYS:
            future_weekdays.append(d)

    total_filled = 0
    for crew_name in all_crew_names:
        crew_info = CREWS[crew_name]
        depot = crew_info["depot"]
        members = crew_info["members"]
        locations = DEPOT_LOCATIONS.get(depot, [depot])
        primary_type = crew_info["type"]
        existing_dates = crew_dates[crew_name]

        for d in future_weekdays:
            if d in existing_dates:
                continue

            # Add 1-2 WOs for this missing day
            n_fill = random.randint(1, 2)
            for _ in range(n_fill):
                templates = WO_TEMPLATES.get(primary_type, WO_TEMPLATES["Planned Maintenance"])
                title_tpl, desc_tpl = random.choice(templates)
                loc = random.choice(locations)
                title = title_tpl.format(loc=loc)
                wo_number = f"WO-{today.year}-{wo_id:05d}"
                created = d - timedelta(days=random.randint(1, 5))

                all_wos.append({
                    "id": wo_id, "wo_number": wo_number,
                    "project_id": random.randint(1, 120),
                    "asset_id": _pick_asset_id(primary_type, depot),
                    "title": title.replace("'", "''"), "wo_type": primary_type,
                    "priority": random.choices(PRIORITIES, weights=PRIORITY_WEIGHTS)[0],
                    "status": "Open", "created_date": created.isoformat(),
                    "scheduled_date": d.isoformat(), "completed_date": None,
                    "estimated_hours": round(random.uniform(2, 8), 1),
                    "actual_hours": None, "assigned_crew": crew_name,
                    "location": loc, "description": desc_tpl.replace("'", "''"),
                })
                for seq in range(1, 3):
                    assigned = random.choice(members)
                    all_tasks.append({
                        "id": task_id, "work_order_id": wo_id,
                        "task_number": f"T-{task_id:06d}", "sequence": seq,
                        "task_name": f"Task {seq}", "status": "Open",
                        "scheduled_datetime": d.isoformat(), "completed_datetime": None,
                        "assigned_to": assigned["name"].replace("'", "''"),
                        "estimated_hours": round(random.uniform(1, 4), 1),
                        "actual_hours": None, "notes": None,
                    })
                    task_id += 1
                wo_id += 1
            total_filled += 1

    print(f"  Filled {total_filled} crew-day gaps across {len(all_crew_names)} crews x {len(future_weekdays)} weekdays")

    print(f"  Generated {len(all_wos)} work orders and {len(all_tasks)} tasks")
    return all_wos, all_tasks


def insert_work_orders(wos: list[dict]):
    """Insert work orders in batches."""
    print(f"\n=== Inserting {len(wos)} work orders ===")

    # Delete existing data in the future range to avoid duplicates
    run_sql(f"DELETE FROM {UC_FULL}.work_orders WHERE id >= 10000")
    print("  Cleared old generated data")

    batch_size = 50
    for i in range(0, len(wos), batch_size):
        batch = wos[i:i+batch_size]
        values = []
        for w in batch:
            cd = f"'{w['completed_date']}'" if w["completed_date"] else "NULL"
            ah = w["actual_hours"] if w["actual_hours"] is not None else "NULL"
            desc = w['description'].replace("'", "''") if w['description'] else ""
            title = w['title'].replace("'", "''")
            crew = w['assigned_crew'].replace("'", "''")
            loc = w.get('location', '').replace("'", "''")
            values.append(
                f"({w['id']}, '{w['wo_number']}', {w['project_id']}, {w['asset_id']}, "
                f"'{title}', '{w['wo_type']}', '{w['priority']}', '{w['status']}', "
                f"'{w['created_date']}', '{w['scheduled_date']}', {cd}, "
                f"{w['estimated_hours']}, {ah}, '{crew}', '{desc}', '{loc}')"
            )
        sql = f"INSERT INTO {UC_FULL}.work_orders (id, wo_number, project_id, asset_id, title, wo_type, priority, status, created_date, scheduled_date, completed_date, estimated_hours, actual_hours, assigned_crew, description, location) VALUES {', '.join(values)}"
        result = run_sql(sql)
        if result is None:
            print(f"  Batch {i//batch_size + 1}: FAILED")
        else:
            print(f"  Batch {i//batch_size + 1}: {len(batch)} rows OK")


def insert_tasks(tasks: list[dict]):
    """Insert tasks in batches."""
    print(f"\n=== Inserting {len(tasks)} tasks ===")

    run_sql(f"DELETE FROM {UC_FULL}.work_tasks WHERE id >= 50000")
    print("  Cleared old generated data")

    batch_size = 100
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        values = []
        for t in batch:
            cd = f"'{t['completed_datetime']}'" if t["completed_datetime"] else "NULL"
            ah = t["actual_hours"] if t["actual_hours"] is not None else "NULL"
            notes_raw = t['notes'].replace("'", "''") if t['notes'] else ""
            notes = f"'{notes_raw}'" if t['notes'] else "NULL"
            tname = t['task_name'].replace("'", "''")
            assigned = t['assigned_to'].replace("'", "''")
            values.append(
                f"({t['id']}, {t['work_order_id']}, '{t['task_number']}', {t['sequence']}, "
                f"'{tname}', '{t['status']}', "
                f"'{t['scheduled_datetime']}', {cd}, '{assigned}', "
                f"{t['estimated_hours']}, {ah}, {notes})"
            )
        sql = f"INSERT INTO {UC_FULL}.work_tasks VALUES {', '.join(values)}"
        result = run_sql(sql)
        if result is None:
            print(f"  Batch {i//batch_size + 1}: FAILED")
        else:
            print(f"  Batch {i//batch_size + 1}: {len(batch)} rows OK")


def optimize_tables():
    """Apply liquid clustering and OPTIMIZE for query performance."""
    print("\n=== Optimizing tables ===")

    print("  Liquid clustering: work_orders (assigned_crew, scheduled_date)")
    run_sql(f"ALTER TABLE {UC_FULL}.work_orders CLUSTER BY (assigned_crew, scheduled_date)")

    print("  Liquid clustering: work_tasks (work_order_id, scheduled_datetime)")
    run_sql(f"ALTER TABLE {UC_FULL}.work_tasks CLUSTER BY (work_order_id, scheduled_datetime)")

    print("  OPTIMIZE work_orders...")
    run_sql(f"OPTIMIZE {UC_FULL}.work_orders")

    print("  OPTIMIZE work_tasks...")
    run_sql(f"OPTIMIZE {UC_FULL}.work_tasks")

    print("  Done.")


def update_system_prompt_date_note():
    """Print the updated crew names for the agent system prompt."""
    crew_names = sorted(CREWS.keys())
    print("\n=== Crew names for system prompt ===")
    print(", ".join(crew_names))


if __name__ == "__main__":
    print("=" * 60)
    print("Generating realistic operations data for demo")
    print(f"Date range: 2026-03-01 to 2026-04-15")
    print(f"Easter break: 2026-04-03 to 2026-04-06")
    print("=" * 60)

    create_tables()
    generate_asset_types()
    assets = generate_assets()
    generate_asset_images()
    wos, tasks = generate_work_orders()
    insert_work_orders(wos)
    insert_tasks(tasks)
    optimize_tables()

    print("\n=== Done ===")
    print(f"Total: {len(assets)} assets, {len(wos)} work orders, {len(tasks)} tasks")
