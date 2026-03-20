"""Generate realistic WACS data for EE Crew Briefing demo.

Creates depot-based crews with real Australian names, historical work orders
for March 2026 and scheduled work through mid-April 2026.
Easter 2026: Good Friday 3 Apr, Easter Saturday 4 Apr, Easter Monday 6 Apr.

Run with: python3 setup/05_realistic_data.py
"""

import subprocess
import json
import random
from datetime import date, timedelta, datetime

PROFILE = "DEFAULT"
WAREHOUSE_ID = "c2abb17a6c9e6bc0"
SCHEMA = "zivile.essential_energy_wacs"

# ── Easter 2026 public holidays ──
EASTER_DATES = {date(2026, 4, 3), date(2026, 4, 4), date(2026, 4, 5), date(2026, 4, 6)}
# Other NSW public holidays in range
NSW_HOLIDAYS = EASTER_DATES | {
    date(2026, 3, 2),   # Bank Holiday (hypothetical Canberra Day region)
}

random.seed(42)  # reproducible

# ── Realistic depot-based crews ──
CREWS = {
    # Depot: Grafton (Coastal region)
    "Grafton Lines A": {
        "depot": "Grafton", "type": "Planned Maintenance",
        "members": [
            {"name": "Greg Thompson", "role": "Crew Leader"},
            {"name": "Shane Murray", "role": "Powerline Worker"},
            {"name": "Ben O'Brien", "role": "Powerline Worker"},
            {"name": "Lachlan Mitchell", "role": "Apprentice"},
        ],
    },
    "Grafton Lines B": {
        "depot": "Grafton", "type": "Corrective Maintenance",
        "members": [
            {"name": "Darren Walsh", "role": "Crew Leader"},
            {"name": "Matt Sullivan", "role": "Powerline Worker"},
            {"name": "Cooper Ryan", "role": "Powerline Worker"},
        ],
    },
    # Depot: Coffs Harbour (Coastal)
    "Coffs Harbour Lines": {
        "depot": "Coffs Harbour", "type": "Planned Maintenance",
        "members": [
            {"name": "Craig Williams", "role": "Crew Leader"},
            {"name": "Jason Kelly", "role": "Powerline Worker"},
            {"name": "Riley Brown", "role": "Powerline Worker"},
            {"name": "Jack Anderson", "role": "Apprentice"},
        ],
    },
    "Coffs Harbour Cable": {
        "depot": "Coffs Harbour", "type": "Capital Works",
        "members": [
            {"name": "Adam Stewart", "role": "Senior Cable Jointer"},
            {"name": "Nathan Chen", "role": "Cable Jointer"},
        ],
    },
    # Depot: Lismore (Coastal)
    "Lismore Lines": {
        "depot": "Lismore", "type": "Corrective Maintenance",
        "members": [
            {"name": "Wayne Campbell", "role": "Crew Leader"},
            {"name": "Josh Henderson", "role": "Powerline Worker"},
            {"name": "Ethan Taylor", "role": "Powerline Worker"},
        ],
    },
    # Depot: Port Macquarie (Mid North Coast)
    "Port Macquarie Lines": {
        "depot": "Port Macquarie", "type": "Planned Maintenance",
        "members": [
            {"name": "Brett Robertson", "role": "Crew Leader"},
            {"name": "Chris Johnson", "role": "Powerline Worker"},
            {"name": "Thomas Wilson", "role": "Powerline Worker"},
            {"name": "Bailey Smith", "role": "Apprentice"},
        ],
    },
    # Depot: Tamworth (Northern)
    "Tamworth Lines": {
        "depot": "Tamworth", "type": "Planned Maintenance",
        "members": [
            {"name": "Mick Anderson", "role": "Crew Leader"},
            {"name": "Luke Campbell", "role": "Powerline Worker"},
            {"name": "Ryan Singh", "role": "Powerline Worker"},
        ],
    },
    "Tamworth Substation": {
        "depot": "Tamworth", "type": "Capital Works",
        "members": [
            {"name": "Andrew McPherson", "role": "Senior Electrical Tech"},
            {"name": "Noah Williams", "role": "Electrical Tech"},
        ],
    },
    # Depot: Armidale (Northern Tablelands)
    "Armidale Lines": {
        "depot": "Armidale", "type": "Planned Maintenance",
        "members": [
            {"name": "Col Stewart", "role": "Crew Leader"},
            {"name": "Liam Brown", "role": "Powerline Worker"},
            {"name": "Will Thompson", "role": "Powerline Worker"},
        ],
    },
    "Armidale Inspection": {
        "depot": "Armidale", "type": "Inspection",
        "members": [
            {"name": "Sarah Mitchell", "role": "Senior Inspector"},
            {"name": "Emma Walsh", "role": "Asset Inspector"},
            {"name": "Brooke Nguyen", "role": "Asset Inspector"},
        ],
    },
    # Depot: Orange (Western/Ranges)
    "Orange Lines": {
        "depot": "Orange", "type": "Planned Maintenance",
        "members": [
            {"name": "Gary Wilson", "role": "Crew Leader"},
            {"name": "Kevin Murray", "role": "Powerline Worker"},
            {"name": "Josh Patel", "role": "Powerline Worker"},
            {"name": "Jack Sullivan", "role": "Apprentice"},
        ],
    },
    "Orange Cable": {
        "depot": "Orange", "type": "Capital Works",
        "members": [
            {"name": "Nicole Henderson", "role": "Senior Cable Jointer"},
            {"name": "Matt Robertson", "role": "Cable Jointer"},
        ],
    },
    # Depot: Dubbo (Western/Macquarie)
    "Dubbo Lines": {
        "depot": "Dubbo", "type": "Planned Maintenance",
        "members": [
            {"name": "Shane Taylor", "role": "Crew Leader"},
            {"name": "Ben Williams", "role": "Powerline Worker"},
            {"name": "Cooper Johnson", "role": "Powerline Worker"},
        ],
    },
    "Dubbo Emergency": {
        "depot": "Dubbo", "type": "Emergency Response",
        "members": [
            {"name": "Darren Brown", "role": "Crew Leader"},
            {"name": "Ryan Kelly", "role": "Powerline Worker"},
        ],
    },
    # Depot: Bathurst (Ranges)
    "Bathurst Lines": {
        "depot": "Bathurst", "type": "Planned Maintenance",
        "members": [
            {"name": "Craig Anderson", "role": "Crew Leader"},
            {"name": "Jason Thompson", "role": "Powerline Worker"},
            {"name": "Riley Mitchell", "role": "Apprentice"},
        ],
    },
    # Depot: Wagga Wagga (Riverina)
    "Wagga Wagga Lines": {
        "depot": "Wagga Wagga", "type": "Planned Maintenance",
        "members": [
            {"name": "Adam Walsh", "role": "Crew Leader"},
            {"name": "Nathan Murray", "role": "Powerline Worker"},
            {"name": "Ethan Stewart", "role": "Powerline Worker"},
            {"name": "Lachlan Wilson", "role": "Apprentice"},
        ],
    },
    "Wagga Wagga Inspection": {
        "depot": "Wagga Wagga", "type": "Inspection",
        "members": [
            {"name": "Kylie Robertson", "role": "Senior Inspector"},
            {"name": "Tegan Campbell", "role": "Asset Inspector"},
        ],
    },
    # Depot: Broken Hill (Far West)
    "Broken Hill Lines": {
        "depot": "Broken Hill", "type": "Planned Maintenance",
        "members": [
            {"name": "Mick O'Brien", "role": "Crew Leader"},
            {"name": "Luke Henderson", "role": "Powerline Worker"},
        ],
    },
    # Depot: Moree (North Western)
    "Moree Lines": {
        "depot": "Moree", "type": "Planned Maintenance",
        "members": [
            {"name": "Brett Singh", "role": "Crew Leader"},
            {"name": "Chris Sullivan", "role": "Powerline Worker"},
        ],
    },
    # Depot: Mudgee (Macquarie)
    "Mudgee Lines": {
        "depot": "Mudgee", "type": "Corrective Maintenance",
        "members": [
            {"name": "Wayne Anderson", "role": "Crew Leader"},
            {"name": "Josh Murray", "role": "Powerline Worker"},
        ],
    },
    # Depot: Inverell (Northern Tablelands)
    "Inverell Lines": {
        "depot": "Inverell", "type": "Planned Maintenance",
        "members": [
            {"name": "Kevin Brown", "role": "Crew Leader"},
            {"name": "Thomas Kelly", "role": "Powerline Worker"},
        ],
    },
    # Depot: Queanbeyan (South Eastern)
    "Queanbeyan Lines": {
        "depot": "Queanbeyan", "type": "Planned Maintenance",
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

# Locations per depot for work order titles
DEPOT_LOCATIONS = {
    "Grafton": ["Grafton", "South Grafton", "Junction Hill", "Ulmarra", "Maclean", "Yamba"],
    "Coffs Harbour": ["Coffs Harbour", "Sawtell", "Toormina", "Woolgoolga", "Bellingen"],
    "Lismore": ["Lismore", "Goonellabah", "Ballina", "Byron Bay", "Alstonville"],
    "Port Macquarie": ["Port Macquarie", "Wauchope", "Laurieton", "Kempsey", "Nambucca Heads"],
    "Tamworth": ["Tamworth", "Gunnedah", "Quirindi", "Werris Creek", "Manilla"],
    "Armidale": ["Armidale", "Uralla", "Walcha", "Guyra", "Dorrigo"],
    "Orange": ["Orange", "Blayney", "Millthorpe", "Molong", "Canowindra"],
    "Dubbo": ["Dubbo", "Wellington", "Narromine", "Peak Hill", "Gilgandra"],
    "Bathurst": ["Bathurst", "Kelso", "Raglan", "Sofala", "Portland"],
    "Wagga Wagga": ["Wagga Wagga", "Junee", "Cootamundra", "Temora", "Tumut"],
    "Broken Hill": ["Broken Hill", "Menindee", "Silverton", "Wilcannia"],
    "Moree": ["Moree", "Narrabri", "Wee Waa", "Boggabri"],
    "Mudgee": ["Mudgee", "Gulgong", "Kandos", "Rylstone"],
    "Inverell": ["Inverell", "Ashford", "Bundarra", "Tingha"],
    "Queanbeyan": ["Queanbeyan", "Bungendore", "Braidwood", "Yass", "Cooma"],
    "Various": ["Grafton", "Coffs Harbour", "Tamworth", "Orange", "Dubbo", "Wagga Wagga"],
}


def run_sql(statement: str, silent=False):
    payload = json.dumps({"statement": statement, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "50s"})
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements/", "--json", payload, "--profile", PROFILE],
        capture_output=True, text=True,
    )
    if result.stdout.strip():
        try:
            r = json.loads(result.stdout)
        except json.JSONDecodeError:
            if not silent:
                print(f"  JSON parse error: {result.stdout[:200]}")
            return None
        state = r.get("status", {}).get("state", "")
        if state == "FAILED":
            msg = r.get("status", {}).get("error", {}).get("message", "")
            print(f"  SQL FAILED: {msg[:300]}")
            return None
        return r
    if result.stderr and not silent:
        print(f"  ERROR: {result.stderr[:200]}")
    return None


def is_workday(d: date) -> bool:
    return d.weekday() < 5 and d not in EASTER_DATES and d not in NSW_HOLIDAYS


def generate_work_orders():
    """Generate work orders from 1 March to 15 April 2026."""
    print("\n=== Generating work orders ===")

    start_date = date(2026, 3, 1)
    end_date = date(2026, 4, 15)
    today = date(2026, 3, 20)

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

                wo_number = f"WO-2026-{wo_id:05d}"
                created = d - timedelta(days=random.randint(1, 14))

                all_wos.append({
                    "id": wo_id,
                    "wo_number": wo_number,
                    "project_id": random.randint(1, 120),
                    "asset_id": random.randint(1, 14795),
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

    print(f"  Generated {len(all_wos)} work orders and {len(all_tasks)} tasks")
    return all_wos, all_tasks


def insert_work_orders(wos: list[dict]):
    """Insert work orders in batches."""
    print(f"\n=== Inserting {len(wos)} work orders ===")

    # Delete existing data in the future range to avoid duplicates
    run_sql(f"DELETE FROM {SCHEMA}.work_orders WHERE id >= 10000")
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
            values.append(
                f"({w['id']}, '{w['wo_number']}', {w['project_id']}, {w['asset_id']}, "
                f"'{title}', '{w['wo_type']}', '{w['priority']}', '{w['status']}', "
                f"'{w['created_date']}', '{w['scheduled_date']}', {cd}, "
                f"{w['estimated_hours']}, {ah}, '{crew}', '{desc}')"
            )
        sql = f"INSERT INTO {SCHEMA}.work_orders VALUES {', '.join(values)}"
        result = run_sql(sql)
        if result is None:
            print(f"  Batch {i//batch_size + 1}: FAILED")
        else:
            print(f"  Batch {i//batch_size + 1}: {len(batch)} rows OK")


def insert_tasks(tasks: list[dict]):
    """Insert tasks in batches."""
    print(f"\n=== Inserting {len(tasks)} tasks ===")

    run_sql(f"DELETE FROM {SCHEMA}.work_tasks WHERE id >= 50000")
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
        sql = f"INSERT INTO {SCHEMA}.work_tasks VALUES {', '.join(values)}"
        result = run_sql(sql)
        if result is None:
            print(f"  Batch {i//batch_size + 1}: FAILED")
        else:
            print(f"  Batch {i//batch_size + 1}: {len(batch)} rows OK")


def update_system_prompt_date_note():
    """Print the updated crew names for the agent system prompt."""
    crew_names = sorted(CREWS.keys())
    print("\n=== Crew names for system prompt ===")
    print(", ".join(crew_names))


if __name__ == "__main__":
    print("=" * 60)
    print("Generating realistic WACS data for demo")
    print(f"Date range: 2026-03-01 to 2026-04-15")
    print(f"Easter break: 2026-04-03 to 2026-04-06")
    print("=" * 60)

    wos, tasks = generate_work_orders()
    insert_work_orders(wos)
    insert_tasks(tasks)
    update_system_prompt_date_note()

    print("\n=== Done ===")
    print(f"Total: {len(wos)} work orders, {len(tasks)} tasks")
