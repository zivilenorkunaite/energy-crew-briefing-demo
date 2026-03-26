"""SWMS content — single source of truth for all Safe Work Method Statements.

Generic Australian electricity distribution content. No company-specific branding.
References AS/NZS standards, WHS Act 2011, and SafeWork Australia guidance.

Used by:
- setup/11_seed_swms.py (seeds Delta table)
- generate_swms_pdfs.py (PDF generation)
- server/swms.py (DOCUMENT_NAMES list)
"""

SWMS_CONTENT = {
    "SWMS-001 Asset Replacement": {
        "SCOPE": (
            "Replacement of distribution network assets including power transformers, "
            "pole-mount transformers, switchgear, reclosers, sectionalisers, poles, "
            "cross-arms, insulators, and associated hardware.\n"
            "Applies to both overhead and underground network assets rated up to 33kV."
        ),
        "HAZARDS": (
            "- Electrical shock or electrocution from contact with energised conductors\n"
            "- Arc flash and arc blast during switching or disconnection operations\n"
            "- Falls from height during pole-top or elevated platform work (>2m)\n"
            "- Crush injuries from heavy equipment (transformers up to 2,500kg)\n"
            "- Manual handling injuries from lifting poles, cross-arms, hardware\n"
            "- Vehicle and mobile plant interaction (EWP, crane, truck)\n"
            "- Environmental hazards: extreme heat (>35°C), UV exposure, snakes, insects\n"
            "- Dropped objects from height onto ground workers\n"
            "- Underground services strike during excavation for pad-mount assets\n"
            "- PCB or oil contamination from older transformer units"
        ),
        "RISK CONTROLS": (
            "- Obtain Access Permit and Switching Program before commencing\n"
            "- Confirm isolation and prove dead at point of work using approved voltage tester\n"
            "- Apply personal earths and bonds per NENS-10 requirements\n"
            "- Establish exclusion zones: 3m for 11kV, 6m for 33kV where live conductors present\n"
            "- Use rated lifting equipment with current certification (crane, EWP, gin pole)\n"
            "- Two-person minimum for all pole-top and heavy-lift operations\n"
            "- Spotter required for all mobile plant movements on site\n"
            "- Continuous communication via two-way radio between ground and elevated crews\n"
            "- Dial Before You Dig (DBYD) clearance for any excavation work\n"
            "- Environmental controls: oil containment bunding, spill kit on site"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat (AS/NZS 1801) with chin strap for elevated work\n"
            "- Safety glasses (AS/NZS 1337) — clear lens for general, tinted for outdoor\n"
            "- Arc-rated flash suit (minimum 8 cal/cm²) for switching operations\n"
            "- HV insulating gloves (Class 0 for LV, Class 2 for 11kV, Class 3 for 33kV) with leather protectors\n"
            "- FR (flame-resistant) long-sleeve shirt and trousers\n"
            "- Steel-cap safety boots (AS/NZS 2210.3) with electrical hazard rating\n"
            "- Full-body harness with twin-tail lanyard for work at height (AS/NZS 1891)\n"
            "- Hearing protection where noise exceeds 85dB(A)\n"
            "- Sunscreen SPF 50+ and wide-brim hat attachment for outdoor work"
        ),
        "ISOLATION PROCEDURES": (
            "- Submit isolation request via Network Operations Centre (NOC) minimum 5 business days prior\n"
            "- Switching Program to be prepared by authorised switching operator\n"
            "- Confirm isolation at point of work — test for dead using approved voltage detection device\n"
            "- Apply personal safety earths and bonds before commencing work\n"
            "- Maintain Access Permit register with names and contact details\n"
            "- All workers to sign on/off Access Permit at commencement and completion\n"
            "- Permit holder to confirm all workers clear before re-energisation\n"
            "- Live-line work only with specific authorisation and live-line trained crew"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Current electrical licence (Powerline Worker or equivalent)\n"
            "- Authorisation to Work on the distribution network (current endorsement)\n"
            "- High Voltage Switching Competency (for switching operations)\n"
            "- Elevated Work Platform (EWP) operator ticket (current)\n"
            "- Working at Heights competency (RIIWHS204E or equivalent)\n"
            "- Crane operation dogger/rigger ticket where applicable\n"
            "- First Aid certificate (HLTAID011) — minimum one per crew\n"
            "- CPR including AED (HLTAID009) — all crew members\n"
            "- Asbestos Awareness for pre-1990 assets (CPCCDE3014)"
        ),
        "EMERGENCY PROCEDURES": (
            "- Crew Leader to brief all workers on emergency plan before starting\n"
            "- Emergency contacts: 000 (ambulance/fire), NOC, depot supervisor\n"
            "- Nearest hospital location and travel time confirmed on pre-start\n"
            "- AED defibrillator and first aid kit accessible on site vehicle\n"
            "- In case of electrical contact: DO NOT touch victim, isolate supply, call 000\n"
            "- Pole-top rescue kit on site for all elevated work\n"
            "- Fire extinguisher (dry chemical) on all vehicles and at work site\n"
            "- Severe weather protocol: suspend work if lightning within 10km or wind >60km/h"
        ),
    },

    "SWMS-002 Capital Works": {
        "SCOPE": (
            "Construction of new distribution network infrastructure including new line "
            "construction, network extensions, feeder augmentation, new substations, "
            "and customer connection works.\n"
            "Covers overhead (OHEW, LVABC, HV bare conductor) and underground (XLPE, pilcon) installations."
        ),
        "HAZARDS": (
            "- Electrical contact with existing energised network during tie-in\n"
            "- Falls from height — pole erection, conductor stringing, hardware installation\n"
            "- Mobile plant — crane, EWP, borer, excavator movements near workers\n"
            "- Overhead conductor tension — uncontrolled release during stringing\n"
            "- Underground services — gas, water, telco during excavation\n"
            "- Traffic hazards where work is adjacent to roadways\n"
            "- Manual handling — poles (up to 600kg), drums, hardware\n"
            "- Environmental — bushfire risk, heat stress, flooding in excavations\n"
            "- Public interaction — pedestrians, livestock, property access"
        ),
        "RISK CONTROLS": (
            "- Traffic Management Plan (TMP) for all road-adjacent work\n"
            "- DBYD clearance and pot-holing before any excavation\n"
            "- Conductor stringing: use calibrated tension equipment, exclusion zone below\n"
            "- Pre-start briefing covering energised assets in proximity\n"
            "- Barricading and signage around all open excavations\n"
            "- Mobile plant exclusion zones: minimum 3m from overhead lines\n"
            "- Spotter for all reversing vehicles and crane operations\n"
            "- Shoring or battering for excavations deeper than 1.5m\n"
            "- Dust suppression where required (water cart or chemical stabiliser)"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat with chin strap (AS/NZS 1801)\n"
            "- Safety glasses — clear and tinted (AS/NZS 1337)\n"
            "- FR long-sleeve shirt and trousers\n"
            "- Steel-cap boots with electrical hazard rating (AS/NZS 2210.3)\n"
            "- Full-body harness with shock absorber for work at height\n"
            "- HV insulating gloves with leather protectors where approaching energised assets\n"
            "- High-visibility vest (Day/Night rated — AS/NZS 4602) for road-adjacent work\n"
            "- Hearing protection near boring or compaction equipment\n"
            "- Gloves (rigger or mechanics) for general handling"
        ),
        "ISOLATION PROCEDURES": (
            "- No work within approach distance of energised conductors without approved isolation\n"
            "- Tie-in to existing network: Switching Program required, isolation confirmed at point of work\n"
            "- New construction treated as de-energised until connected — tag and lock all ends\n"
            "- Parallel conductor hazard: induced voltages on new conductors near live lines\n"
            "- Earth new conductors at both ends during stringing near live assets"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Electrical licence (Powerline Worker or equivalent)\n"
            "- Network Authorisation (current endorsement)\n"
            "- EWP operator ticket and Working at Heights competency\n"
            "- Traffic Controller (RIIWHS302D) for road-adjacent work\n"
            "- Excavator/borer operator tickets where applicable\n"
            "- First Aid and CPR — minimum one per crew\n"
            "- Confined Space Entry (RIIWHS202E) for underground vault work"
        ),
        "EMERGENCY PROCEDURES": (
            "- Emergency muster point identified on site plan\n"
            "- All workers briefed on nearest hospital route\n"
            "- Fallen conductor procedure: 8m exclusion zone, call NOC immediately\n"
            "- Excavation collapse: do not enter, call 000, secure perimeter\n"
            "- Vehicle incident near energised assets: stay in vehicle, call NOC\n"
            "- AED and first aid kit on primary site vehicle"
        ),
    },

    "SWMS-003 Corrective Maintenance": {
        "SCOPE": (
            "Unplanned repair and restoration work on distribution network assets following "
            "equipment failure, fault indication, or damage detection.\n"
            "Includes conductor repairs, fuse replacement, recloser/sectionaliser restoration, "
            "damaged pole stabilisation, and fault-finding activities."
        ),
        "HAZARDS": (
            "- Unknown network state — faults may cause unexpected energisation\n"
            "- Contact with damaged but still-energised equipment\n"
            "- Step and touch potential near fallen or damaged conductors\n"
            "- Structural instability of damaged poles, cross-arms, or structures\n"
            "- Falls from height during urgent repairs on compromised structures\n"
            "- Fatigue-related errors during extended fault restoration shifts\n"
            "- Working at night with limited visibility\n"
            "- Traffic hazards at roadside fault locations\n"
            "- Public proximity — bystanders near damaged infrastructure"
        ),
        "RISK CONTROLS": (
            "- Dynamic risk assessment (Take 5) before approaching damaged assets\n"
            "- Treat all conductors as energised until proven dead\n"
            "- Minimum approach distance maintained at all times\n"
            "- Structural assessment of poles/structures before climbing or attaching EWP\n"
            "- Fatigue management: maximum 14-hour shift, mandatory rest before driving\n"
            "- Night work: adequate lighting (minimum 50 lux at work face)\n"
            "- Traffic control for all road-adjacent fault work (minimum cones + flashing lights)\n"
            "- Two-person minimum for all corrective work on network assets\n"
            "- Continuous communication with NOC for network state updates"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat with chin strap (AS/NZS 1801)\n"
            "- Safety glasses (AS/NZS 1337)\n"
            "- Arc-rated flash suit for fault investigation near energised equipment\n"
            "- HV insulating gloves and leather protectors\n"
            "- FR clothing (shirt and trousers)\n"
            "- Steel-cap boots with electrical hazard rating\n"
            "- Full-body harness for any elevated work\n"
            "- High-visibility vest (Day/Night rated) for roadside work\n"
            "- Head torch for night work (intrinsically safe where gas risk)"
        ),
        "ISOLATION PROCEDURES": (
            "- Emergency switching may be performed by authorised switching operator\n"
            "- Fault isolation: open upstream device, confirm dead at point of work\n"
            "- Apply personal earths even for emergency repairs\n"
            "- Back-feed risk assessment — check for embedded generation (solar, batteries)\n"
            "- Re-energisation only after Crew Leader confirms all workers clear"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Electrical licence with current network authorisation\n"
            "- Fault-finding and diagnostic competency\n"
            "- Emergency switching authority (for first responder switching)\n"
            "- Working at Heights and EWP operator ticket\n"
            "- First Aid and CPR — all crew members\n"
            "- Night work induction (where applicable)"
        ),
        "EMERGENCY PROCEDURES": (
            "- Fallen conductor: establish 8m exclusion zone, call NOC, guard until made safe\n"
            "- Electrical contact incident: isolate supply, call 000, commence CPR if needed\n"
            "- Structure failure: evacuate area, establish exclusion zone\n"
            "- Vehicle accident near network assets: remain in vehicle, call NOC\n"
            "- All emergency contacts pre-loaded in crew mobile phones"
        ),
    },

    "SWMS-004 Emergency Response": {
        "SCOPE": (
            "Rapid response to network emergencies including storm damage, bushfire impacts, "
            "vehicle-into-pole incidents, fallen conductors, flood damage, and third-party "
            "contact with network assets.\n"
            "Covers make-safe operations, temporary supply restoration, and public safety management."
        ),
        "HAZARDS": (
            "- Multiple simultaneous hazards in emergency conditions\n"
            "- Energised fallen conductors on ground or in water\n"
            "- Bushfire proximity — radiant heat, smoke, ember attack\n"
            "- Floodwater — submerged infrastructure, access road damage\n"
            "- Structural collapse — poles, trees, buildings on network assets\n"
            "- Extreme fatigue during extended emergency operations\n"
            "- Public panic and unauthorised approach to hazard zones\n"
            "- Communication failure in remote areas or during network overload\n"
            "- Vehicle rollover on damaged roads or off-road access tracks\n"
            "- Psychological stress from exposure to injury or property destruction"
        ),
        "RISK CONTROLS": (
            "- Incident Controller appointed for all multi-crew emergency responses\n"
            "- Dynamic risk assessment at every scene before approaching\n"
            "- Treat all conductors as energised — 8m exclusion zone for fallen lines\n"
            "- Bushfire: monitor conditions via RFS alerts, maintain escape route at all times\n"
            "- Flood: do not enter floodwater, do not drive through water of unknown depth\n"
            "- Fatigue management: crew rotation every 12 hours, welfare checks\n"
            "- Coordinate with emergency services (Fire, Police, SES) at scene\n"
            "- Public safety: barricade, signage, guard until area made safe\n"
            "- Satellite phone or UHF radio as backup communication\n"
            "- Buddy system — no solo work during emergency operations"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat (AS/NZS 1801)\n"
            "- Safety glasses (AS/NZS 1337)\n"
            "- Arc-rated flash suit for switching and energised work\n"
            "- HV insulating gloves with leather protectors\n"
            "- FR clothing (long-sleeve shirt and trousers)\n"
            "- Steel-cap boots with electrical hazard rating\n"
            "- High-visibility vest (Day/Night rated)\n"
            "- Bushfire: additional P2 respirator, goggles, long cotton undergarments\n"
            "- Flood: gumboots, personal flotation device where near deep water\n"
            "- Head torch and personal locator beacon for remote/night work"
        ),
        "ISOLATION PROCEDURES": (
            "- Emergency switching authority for first responder to isolate dangerous equipment\n"
            "- Remote switching via SCADA where available\n"
            "- Manual isolation at nearest upstream device if SCADA unavailable\n"
            "- No restoration of supply until make-safe confirmed by qualified worker\n"
            "- Embedded generation (solar, battery) considered — may back-feed"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Electrical licence with emergency response endorsement\n"
            "- Emergency switching authority\n"
            "- Advanced first aid (HLTAID014) — minimum one per crew\n"
            "- 4WD operation for off-road access\n"
            "- Chainsaw operation (AHCMOM213) for storm debris clearing\n"
            "- Mental health first aid awareness"
        ),
        "EMERGENCY PROCEDURES": (
            "- Life-threatening situation: call 000 immediately, do not attempt rescue from energised equipment\n"
            "- Fallen conductor in public area: guard area, call NOC, warn public to stay 8m away\n"
            "- Bushfire threat to crew: activate Bushfire Survival Plan, retreat to safe refuge\n"
            "- Crew member injury: administer first aid, call 000, notify depot supervisor\n"
            "- All incidents reported to NOC and logged in incident management system"
        ),
    },

    "SWMS-005 Inspection": {
        "SCOPE": (
            "Routine and condition-based inspection of distribution network assets including "
            "visual ground patrols, pole-top inspections, thermographic surveys, drone-based "
            "inspections, and underground asset condition assessment.\n"
            "Covers scheduled inspection programs and targeted fault-finding inspections."
        ),
        "HAZARDS": (
            "- Proximity to energised assets during visual inspection\n"
            "- Falls from height during pole-top or structure inspections\n"
            "- Remote area access — rough terrain, creek crossings, livestock\n"
            "- Dog attacks and wildlife encounters (snakes, spiders) during ground patrols\n"
            "- Heat stress during extended outdoor patrols\n"
            "- Vehicle incidents on unsealed roads or during off-road access\n"
            "- Drone operation hazards — flyaway, battery failure, collision\n"
            "- Vegetation contact — ticks, allergic reactions, scratches\n"
            "- Property access — livestock, dogs, locked gates"
        ),
        "RISK CONTROLS": (
            "- Pre-plan inspection route with terrain and access assessment\n"
            "- Maintain minimum approach distances to energised assets\n"
            "- Binoculars and camera zoom for ground-level assessment where possible\n"
            "- Drone inspections per CASA regulations and operator certification\n"
            "- Two-person team for remote area inspections\n"
            "- GPS tracking and check-in schedule for lone workers\n"
            "- Snake-aware: watch step placement, wear gaiters in grass/bush\n"
            "- Property access notification to landholders 48 hours prior\n"
            "- Vehicle pre-start check for off-road suitability"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat (AS/NZS 1801) when near overhead assets\n"
            "- Safety glasses (AS/NZS 1337)\n"
            "- FR clothing (long-sleeve shirt and trousers)\n"
            "- Steel-cap boots (AS/NZS 2210.3)\n"
            "- Full-body harness for any elevated inspection\n"
            "- High-visibility vest\n"
            "- Sunscreen SPF 50+ and broad-brim hat\n"
            "- Snake gaiters for ground patrols in rural areas\n"
            "- Insect repellent for tick and mosquito protection"
        ),
        "ISOLATION PROCEDURES": (
            "- Routine inspections: no isolation required if minimum approach distance maintained\n"
            "- Pole-top inspection requiring approach to energised assets: isolation and Access Permit required\n"
            "- Thermographic surveys may be conducted on energised assets from safe distance\n"
            "- Drone inspections: maintain 3m clearance from energised conductors"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Asset inspection competency (internal training)\n"
            "- Defect classification and reporting competency\n"
            "- CASA Remote Pilot Licence (RePL) for drone operators\n"
            "- Working at Heights for pole-top inspections\n"
            "- First Aid — minimum one per team\n"
            "- 4WD competency for rural/off-road access"
        ),
        "EMERGENCY PROCEDURES": (
            "- Snake bite: immobilise limb, apply pressure bandage, call 000\n"
            "- Heat stress: move to shade, cool with water, monitor consciousness\n"
            "- Vehicle bogging/rollover: stay with vehicle, use UHF or satellite phone\n"
            "- Defect posing imminent danger: report to NOC immediately for emergency action\n"
            "- All critical defects reported same day"
        ),
    },

    "SWMS-006 Planned Maintenance": {
        "SCOPE": (
            "Scheduled preventive maintenance on distribution network assets including "
            "overhead line maintenance, conductor re-tensioning, hardware tightening, "
            "insulator washing, RCD testing, protection relay calibration, "
            "and switchgear servicing.\n"
            "All work planned with isolation and Access Permits arranged in advance."
        ),
        "HAZARDS": (
            "- Electrical shock from incomplete or failed isolation\n"
            "- Arc flash during testing or re-energisation sequences\n"
            "- Falls from height — poles, EWPs, ladders\n"
            "- Overhead conductor contact while working near adjacent live circuits\n"
            "- Manual handling — hardware, tools, components\n"
            "- Environmental — heat, cold, wind, rain affecting safety\n"
            "- Mobile plant movements on site\n"
            "- Dropped objects from elevated work positions\n"
            "- Insulator washing: high-pressure water near energised assets (if live-wash)"
        ),
        "RISK CONTROLS": (
            "- Pre-job briefing (toolbox talk) covering scope, hazards, controls, and isolation\n"
            "- Verify isolation at point of work — test for dead, apply earths\n"
            "- Adjacent circuit identification — maintain approach distances to live circuits\n"
            "- Wind speed monitoring: suspend elevated work if sustained >40km/h or gusts >60km/h\n"
            "- Tool tethering for all work at height (drop zones barricaded below)\n"
            "- Mechanical lifting aids for components >20kg\n"
            "- Step-back review if conditions change during work\n"
            "- End-of-day inspection: all tools accounted for, work area secured"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat with chin strap (AS/NZS 1801)\n"
            "- Safety glasses (AS/NZS 1337)\n"
            "- FR long-sleeve shirt and trousers\n"
            "- Steel-cap boots with electrical hazard rating (AS/NZS 2210.3)\n"
            "- HV insulating gloves (rated for voltage level) with leather protectors\n"
            "- Full-body harness with twin-tail lanyard for elevated work (AS/NZS 1891)\n"
            "- Hearing protection where noise exceeds 85dB(A)\n"
            "- Arc-rated face shield for switching and testing\n"
            "- Sunscreen SPF 50+ for outdoor work"
        ),
        "ISOLATION PROCEDURES": (
            "- Access Permit obtained from NOC before work begins\n"
            "- Switching Program executed by authorised switching operator\n"
            "- Test for dead at each point of work using approved voltage detection device\n"
            "- Apply personal safety earths and bonds per NENS-10\n"
            "- All workers sign on to Access Permit with contact details\n"
            "- Permit holder conducts headcount before re-energisation request\n"
            "- Adjacent circuit awareness: identify and mark all live circuits in work area"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Electrical licence (Powerline Worker or equivalent)\n"
            "- Current network authorisation endorsement\n"
            "- Working at Heights (RIIWHS204E)\n"
            "- EWP operator ticket (current)\n"
            "- Rescue from Height competency for elevated work crews\n"
            "- First Aid (HLTAID011) and CPR (HLTAID009) — all crew\n"
            "- Protection relay testing competency (where applicable)"
        ),
        "EMERGENCY PROCEDURES": (
            "- Electrical contact: isolate, call 000, commence CPR if no pulse\n"
            "- Fall from height: do not move injured person, call 000, stabilise\n"
            "- Severe weather: suspend work, secure loose equipment, shelter in vehicle\n"
            "- Fire: use extinguisher for small fires, evacuate and call 000 for larger\n"
            "- Pole-top rescue procedure practised by crew before commencing elevated work"
        ),
    },

    "SWMS-007 Vegetation Management": {
        "SCOPE": (
            "Trimming, removal, and management of vegetation encroaching on distribution "
            "network assets. Includes mechanical trimming, hand clearing, herbicide application, "
            "and whole-tree removal near overhead power lines.\n"
            "Work performed under Electricity Supply (Safety and Network Management) Regulation "
            "clearance requirements."
        ),
        "HAZARDS": (
            "- Electrical contact when vegetation falls onto or bridges conductors\n"
            "- Chainsaw injuries — lacerations, kickback\n"
            "- Falls from height — tree climbing, EWP in canopy\n"
            "- Struck by falling branches or trees\n"
            "- Chipper/mulcher entanglement\n"
            "- Traffic hazards during roadside clearing\n"
            "- Allergic reactions — sap, insects, ticks\n"
            "- Noise exposure from chainsaws and chippers\n"
            "- Herbicide exposure during chemical treatment\n"
            "- Unstable trees — root rot, termite damage, storm-weakened"
        ),
        "RISK CONTROLS": (
            "- Drop zone assessment: no personnel within 1.5x tree height of felling zone\n"
            "- Tree stability assessment before climbing or cutting\n"
            "- Cut direction planned to fall AWAY from conductors\n"
            "- Spotter monitoring conductor clearance during all cutting operations\n"
            "- Traffic management for roadside vegetation work\n"
            "- Chipper: lockout during blockage clearing, no loose clothing\n"
            "- Herbicide: SDS reviewed, appropriate application method, buffer zones\n"
            "- Two-way radio communication between cutter and spotter\n"
            "- Stop work if wind changes direction during felling operations"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat with face mesh visor (AS/NZS 1801)\n"
            "- Safety glasses (AS/NZS 1337)\n"
            "- Chainsaw chaps or trousers (AS/NZS 4453.3)\n"
            "- Steel-cap boots with cut protection (AS/NZS 2210.3)\n"
            "- Hearing protection — earmuffs rated for chainsaw noise (AS/NZS 1270)\n"
            "- Full-body harness for tree climbing or EWP work\n"
            "- High-visibility vest for roadside work\n"
            "- Rigger gloves for handling branches\n"
            "- Chemical-resistant gloves and goggles for herbicide application\n"
            "- Insect repellent and tick-check protocol"
        ),
        "ISOLATION PROCEDURES": (
            "- Vegetation trimming within approach distance requires network isolation\n"
            "- Minimum clearance distances: 1.5m for LV, 2.5m for 11kV, 4m for 33kV (trimming distances)\n"
            "- If tree has potential to fall onto conductors: isolation required before felling\n"
            "- Contact NOC if vegetation is in contact with or bridging conductors — do not approach"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Arborist qualification (AQF Level 3 minimum)\n"
            "- Chainsaw competency (AHCMOM213 — Operate and maintain chainsaws)\n"
            "- Electrical awareness for vegetation workers (network-specific induction)\n"
            "- EWP operator ticket for elevated platform work\n"
            "- Tree climbing competency with rescue capability\n"
            "- Chemical application licence for herbicide work\n"
            "- First Aid and CPR"
        ),
        "EMERGENCY PROCEDURES": (
            "- Tree contacts conductor: evacuate area, call NOC, 8m exclusion zone\n"
            "- Chainsaw injury: apply pressure, tourniquet if severe, call 000\n"
            "- Fall from tree/EWP: do not move victim, stabilise, call 000\n"
            "- Anaphylaxis (bee/wasp sting): administer EpiPen if available, call 000\n"
            "- All crew carry personal first aid kit with compression bandages"
        ),
    },

    "SWMS-008 Underground Cable": {
        "SCOPE": (
            "Installation, repair, jointing, and testing of underground power cables "
            "including XLPE, pilcon, and paper-insulated lead-covered (PILC) cables "
            "rated up to 33kV.\n"
            "Covers trenching, cable pulling, jointing, termination, and commissioning."
        ),
        "HAZARDS": (
            "- Electrical shock from contact with energised cables or adjacent services\n"
            "- Trench collapse — unstable soils, vibration from nearby traffic\n"
            "- Underground service strike — gas, water, telecommunications\n"
            "- Confined space in underground pits and cable tunnels\n"
            "- Exposure to asbestos (older pit lids and cable insulation)\n"
            "- Manual handling — cable drums (up to 3,000kg), pit lids\n"
            "- Heat from cable jointing equipment (gas torches, heat shrink)\n"
            "- Traffic hazards where trenching adjacent to roads\n"
            "- Silica dust from concrete cutting"
        ),
        "RISK CONTROLS": (
            "- DBYD clearance and pot-holing for all excavation\n"
            "- Trench shoring or battering for excavations >1.5m deep\n"
            "- Cable identification: trace and prove identity before cutting\n"
            "- Confined space entry permit for underground pits (atmosphere testing)\n"
            "- Asbestos register checked — if suspected, stop and engage specialist\n"
            "- Mechanical lifting for cable drums and heavy pit lids\n"
            "- Traffic management plan for road-adjacent trenching\n"
            "- Wet weather: monitor trench for water ingress, dewater before entry\n"
            "- Cable pulling tension monitored — do not exceed manufacturer limits"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat (AS/NZS 1801)\n"
            "- Safety glasses (AS/NZS 1337)\n"
            "- FR clothing for jointing work\n"
            "- Steel-cap boots (AS/NZS 2210.3)\n"
            "- HV insulating gloves for cable identification and testing\n"
            "- P2 respirator for concrete cutting or suspected asbestos\n"
            "- High-visibility vest for roadside work\n"
            "- Rigger gloves for cable handling\n"
            "- Hearing protection during concrete cutting"
        ),
        "ISOLATION PROCEDURES": (
            "- Cable identification: use cable identifier instrument, confirm with NOC\n"
            "- Isolation at both ends of cable before jointing or cutting\n"
            "- Spike test (cable piercing) before cutting unknown cables\n"
            "- Apply earths at point of work after proving dead\n"
            "- Adjacent cable awareness — other cables in same trench may be energised"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Electrical licence (Cable Jointer or equivalent)\n"
            "- Cable jointing competency for specific cable types (XLPE, PILC)\n"
            "- Confined Space Entry (RIIWHS202E)\n"
            "- Excavator operator ticket (where operating plant)\n"
            "- Asbestos Awareness (CPCCDE3014)\n"
            "- First Aid and CPR"
        ),
        "EMERGENCY PROCEDURES": (
            "- Cable strike: evacuate trench, call NOC and 000 if gas detected\n"
            "- Trench collapse: do not enter, call 000, secure perimeter\n"
            "- Confined space rescue: call 000, do not enter without rescue team\n"
            "- Electrical contact in pit: isolate supply at remote end, call 000"
        ),
    },

    "SWMS-009 Metering": {
        "SCOPE": (
            "Installation, replacement, testing, and maintenance of electricity meters "
            "including single-phase, three-phase, CT metering, smart meters, and "
            "solar/battery bi-directional meters.\n"
            "Work performed at customer premises on the meter panel and service connection."
        ),
        "HAZARDS": (
            "- Electrical shock from energised meter panels and service connections\n"
            "- Arc flash at meter board — short circuit during meter exchange\n"
            "- Asbestos in older meter boards (pre-1990 buildings)\n"
            "- Customer premises hazards — dogs, confined spaces, poor access\n"
            "- Working in ceiling spaces for CT metering\n"
            "- Manual handling — meter boxes, CT cabinets\n"
            "- Hostile customers during disconnection work\n"
            "- Biological hazards — needle sticks, animal faeces in meter boxes"
        ),
        "RISK CONTROLS": (
            "- Dynamic risk assessment at each premises before commencing\n"
            "- Check meter board condition — refuse work if board is damaged or unsafe\n"
            "- Use insulated tools rated for 1000V for all meter work\n"
            "- Asbestos: visual check of meter board material, do not drill/cut if suspected\n"
            "- Dog check: if dog present and unrestrained, do not enter — reschedule\n"
            "- Two-person team for CT metering and disconnection work\n"
            "- Customer notification before arrival\n"
            "- Needle-safe protocol: check meter box visually before reaching inside"
        ),
        "PPE REQUIREMENTS": (
            "- Safety glasses (AS/NZS 1337)\n"
            "- LV insulating gloves (Class 00 or Class 0) with leather protectors\n"
            "- FR clothing (long-sleeve shirt)\n"
            "- Steel-cap boots (AS/NZS 2210.3)\n"
            "- Arc-rated face shield for meter exchange (minimum 4 cal/cm²)\n"
            "- P2 respirator if asbestos suspected\n"
            "- Knee pads for ground-level meter work"
        ),
        "ISOLATION PROCEDURES": (
            "- Service fuse removal or main switch off before meter exchange\n"
            "- Verify dead at meter terminals before handling\n"
            "- CT metering: short-circuit CTs before disconnecting — open CT circuit is lethal\n"
            "- Solar/battery systems: isolate DC and AC sides before meter work\n"
            "- Re-energisation: confirm all connections tight, test, inform customer"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Electrical licence\n"
            "- Metering competency (meter types, CT ratios, smart meter commissioning)\n"
            "- Accredited Service Provider (ASP) Level 2 authorisation\n"
            "- Asbestos Awareness (CPCCDE3014)\n"
            "- First Aid and CPR\n"
            "- Customer interaction training"
        ),
        "EMERGENCY PROCEDURES": (
            "- Arc flash at meter board: move to safe distance, call 000 if injured\n"
            "- Electrical contact: isolate by removing service fuse, call 000\n"
            "- Hostile customer: leave premises, call police if threatened\n"
            "- Needle stick: wash wound, report to supervisor, attend medical assessment"
        ),
    },

    "SWMS-010 Substation Work": {
        "SCOPE": (
            "Maintenance, testing, and operation of zone substations and distribution "
            "substations including high-voltage switching, transformer maintenance, "
            "circuit breaker servicing, protection relay testing, battery maintenance, "
            "and earthing system inspection.\n"
            "Covers equipment rated from 415V to 66kV."
        ),
        "HAZARDS": (
            "- High-voltage electrical shock and electrocution\n"
            "- Arc flash and arc blast — energy levels up to 40 cal/cm² at HV\n"
            "- SF6 gas exposure from circuit breaker failures\n"
            "- Transformer oil — PCB contamination in pre-1980 units, oil spills\n"
            "- Battery hazards — acid burns, hydrogen gas explosion risk\n"
            "- Noise from operating transformers and cooling fans\n"
            "- Confined spaces in cable cellars and enclosed switchrooms\n"
            "- Electromagnetic fields near large transformers\n"
            "- Security — unauthorised access to energised substation\n"
            "- Vermin — snakes, rodents, spiders in equipment enclosures"
        ),
        "RISK CONTROLS": (
            "- Substation access control: authorised personnel only, sign-in register\n"
            "- Arc flash study: determine incident energy level, select appropriate PPE category\n"
            "- SF6 gas monitoring during circuit breaker maintenance\n"
            "- Oil containment bunding maintained and tested\n"
            "- Battery room ventilation confirmed before entry — hydrogen monitoring\n"
            "- Two-person minimum for all HV switching operations\n"
            "- Switching Program approved and verified before execution\n"
            "- Earth mat integrity tested annually — touch and step potential within limits\n"
            "- Locked gates and security fencing maintained\n"
            "- Pest inspection before opening enclosures"
        ),
        "PPE REQUIREMENTS": (
            "- Hard hat (AS/NZS 1801)\n"
            "- Safety glasses (AS/NZS 1337)\n"
            "- Arc-rated flash suit — PPE Category 2 minimum (8 cal/cm²), Category 4 (40 cal/cm²) for HV switching\n"
            "- HV insulating gloves (Class 2 for 11kV, Class 3 for 33kV, Class 4 for 66kV)\n"
            "- FR clothing (long-sleeve shirt and trousers)\n"
            "- Steel-cap boots with electrical hazard rating\n"
            "- Face shield with arc rating matching flash suit\n"
            "- Hearing protection in transformer bays\n"
            "- Chemical-resistant gloves and goggles for battery maintenance\n"
            "- Rubber insulating mat for switching operations"
        ),
        "ISOLATION PROCEDURES": (
            "- Formal Switching Program for all HV isolation — approved by switching authority\n"
            "- Five-step switching safety procedure: identify, isolate, prove dead, earth, access\n"
            "- HV voltage detection using approved live-line tester (capacitive or resistive type)\n"
            "- Apply portable earths rated for fault level of installation\n"
            "- Danger tags and locks applied to all isolation points\n"
            "- Adjacent circuit identification — maintain approach distances\n"
            "- No single-person HV switching — observer always present"
        ),
        "COMPETENCY REQUIREMENTS": (
            "- Electrical licence with HV endorsement\n"
            "- HV Switching Authority (current)\n"
            "- Protection relay testing competency\n"
            "- SF6 handling competency\n"
            "- Battery maintenance competency (lead-acid and VRLA)\n"
            "- Confined Space Entry for cable cellars\n"
            "- First Aid (HLTAID011) and CPR with AED (HLTAID009)"
        ),
        "EMERGENCY PROCEDURES": (
            "- HV electrical contact: DO NOT approach, call NOC for remote isolation, then 000\n"
            "- Arc flash incident: evacuate switchroom, call 000, do not re-enter until safe\n"
            "- SF6 leak: evacuate to fresh air, ventilate area, do not re-enter until monitored\n"
            "- Transformer oil spill: contain with bunding/absorbent, notify environmental officer\n"
            "- Battery acid splash: flush with water for 20 minutes, seek medical attention\n"
            "- Substation fire: evacuate, call 000, do not fight fire near energised equipment"
        ),
    },
}

# Derived lists
DOCUMENT_NAMES = list(SWMS_CONTENT.keys())
WORK_TYPES = {
    name.split(" ", 1)[1]: name for name in DOCUMENT_NAMES
}
