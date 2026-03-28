"""
GhostKitchen — Central Reference Data
======================================
Single source of truth for all static platform reference data.
50 kitchens × 10 Texas cities × 3-5 brands per kitchen.

Import this in every generator so kitchen IDs, brands, and zones
stay consistent across orders, sensors, GPS, and seed CSVs.
"""

# ── Brand assignment patterns (3-5 brands per kitchen slot) ──────────────────
# Pattern index 0 → kitchen suffix 01, etc.  Repeats identically per city.
_BP = [
    ["Burger Beast", "Dragon Wok", "Pizza Planet"],                                   # slot 01
    ["Taco Tornado", "Sushi Storm", "Burger Beast"],                                  # slot 02
    ["Pasta Palace", "BBQ Barn", "Dragon Wok", "Salad Studio"],                       # slot 03
    ["Burger Beast", "Pizza Planet", "Taco Tornado", "BBQ Barn"],                     # slot 04
    ["Dragon Wok", "Sushi Storm", "Pasta Palace", "Salad Studio", "Burger Beast"],    # slot 05
]

KITCHENS = [
    # ── Houston ──────────────────────────────────────────────────────────────
    {"kitchen_id": "K-HOU-01", "city": "Houston",       "brands": _BP[0]},
    {"kitchen_id": "K-HOU-02", "city": "Houston",       "brands": _BP[1]},
    {"kitchen_id": "K-HOU-03", "city": "Houston",       "brands": _BP[2]},
    {"kitchen_id": "K-HOU-04", "city": "Houston",       "brands": _BP[3]},
    {"kitchen_id": "K-HOU-05", "city": "Houston",       "brands": _BP[4]},
    # ── Dallas ───────────────────────────────────────────────────────────────
    {"kitchen_id": "K-DAL-01", "city": "Dallas",        "brands": _BP[0]},
    {"kitchen_id": "K-DAL-02", "city": "Dallas",        "brands": _BP[1]},
    {"kitchen_id": "K-DAL-03", "city": "Dallas",        "brands": _BP[2]},
    {"kitchen_id": "K-DAL-04", "city": "Dallas",        "brands": _BP[3]},
    {"kitchen_id": "K-DAL-05", "city": "Dallas",        "brands": _BP[4]},
    # ── Austin ───────────────────────────────────────────────────────────────
    {"kitchen_id": "K-AUS-01", "city": "Austin",        "brands": _BP[0]},
    {"kitchen_id": "K-AUS-02", "city": "Austin",        "brands": _BP[1]},
    {"kitchen_id": "K-AUS-03", "city": "Austin",        "brands": _BP[2]},
    {"kitchen_id": "K-AUS-04", "city": "Austin",        "brands": _BP[3]},
    {"kitchen_id": "K-AUS-05", "city": "Austin",        "brands": _BP[4]},
    # ── San Antonio ──────────────────────────────────────────────────────────
    {"kitchen_id": "K-SAT-01", "city": "San Antonio",   "brands": _BP[0]},
    {"kitchen_id": "K-SAT-02", "city": "San Antonio",   "brands": _BP[1]},
    {"kitchen_id": "K-SAT-03", "city": "San Antonio",   "brands": _BP[2]},
    {"kitchen_id": "K-SAT-04", "city": "San Antonio",   "brands": _BP[3]},
    {"kitchen_id": "K-SAT-05", "city": "San Antonio",   "brands": _BP[4]},
    # ── Fort Worth ───────────────────────────────────────────────────────────
    {"kitchen_id": "K-FTW-01", "city": "Fort Worth",    "brands": _BP[0]},
    {"kitchen_id": "K-FTW-02", "city": "Fort Worth",    "brands": _BP[1]},
    {"kitchen_id": "K-FTW-03", "city": "Fort Worth",    "brands": _BP[2]},
    {"kitchen_id": "K-FTW-04", "city": "Fort Worth",    "brands": _BP[3]},
    {"kitchen_id": "K-FTW-05", "city": "Fort Worth",    "brands": _BP[4]},
    # ── El Paso ──────────────────────────────────────────────────────────────
    {"kitchen_id": "K-ELP-01", "city": "El Paso",       "brands": _BP[0]},
    {"kitchen_id": "K-ELP-02", "city": "El Paso",       "brands": _BP[1]},
    {"kitchen_id": "K-ELP-03", "city": "El Paso",       "brands": _BP[2]},
    {"kitchen_id": "K-ELP-04", "city": "El Paso",       "brands": _BP[3]},
    {"kitchen_id": "K-ELP-05", "city": "El Paso",       "brands": _BP[4]},
    # ── Arlington ────────────────────────────────────────────────────────────
    {"kitchen_id": "K-ARL-01", "city": "Arlington",     "brands": _BP[0]},
    {"kitchen_id": "K-ARL-02", "city": "Arlington",     "brands": _BP[1]},
    {"kitchen_id": "K-ARL-03", "city": "Arlington",     "brands": _BP[2]},
    {"kitchen_id": "K-ARL-04", "city": "Arlington",     "brands": _BP[3]},
    {"kitchen_id": "K-ARL-05", "city": "Arlington",     "brands": _BP[4]},
    # ── Corpus Christi ───────────────────────────────────────────────────────
    {"kitchen_id": "K-CRP-01", "city": "Corpus Christi","brands": _BP[0]},
    {"kitchen_id": "K-CRP-02", "city": "Corpus Christi","brands": _BP[1]},
    {"kitchen_id": "K-CRP-03", "city": "Corpus Christi","brands": _BP[2]},
    {"kitchen_id": "K-CRP-04", "city": "Corpus Christi","brands": _BP[3]},
    {"kitchen_id": "K-CRP-05", "city": "Corpus Christi","brands": _BP[4]},
    # ── Plano ────────────────────────────────────────────────────────────────
    {"kitchen_id": "K-PLN-01", "city": "Plano",         "brands": _BP[0]},
    {"kitchen_id": "K-PLN-02", "city": "Plano",         "brands": _BP[1]},
    {"kitchen_id": "K-PLN-03", "city": "Plano",         "brands": _BP[2]},
    {"kitchen_id": "K-PLN-04", "city": "Plano",         "brands": _BP[3]},
    {"kitchen_id": "K-PLN-05", "city": "Plano",         "brands": _BP[4]},
    # ── Lubbock ──────────────────────────────────────────────────────────────
    {"kitchen_id": "K-LBB-01", "city": "Lubbock",       "brands": _BP[0]},
    {"kitchen_id": "K-LBB-02", "city": "Lubbock",       "brands": _BP[1]},
    {"kitchen_id": "K-LBB-03", "city": "Lubbock",       "brands": _BP[2]},
    {"kitchen_id": "K-LBB-04", "city": "Lubbock",       "brands": _BP[3]},
    {"kitchen_id": "K-LBB-05", "city": "Lubbock",       "brands": _BP[4]},
]

BRANDS = [
    "Burger Beast", "Dragon Wok", "Pizza Planet", "Taco Tornado",
    "Sushi Storm", "Pasta Palace", "BBQ Barn", "Salad Studio",
]

# Zone suffixes — combined with city prefix by generators
DELIVERY_ZONE_SUFFIXES = ["DOWNTOWN", "MIDTOWN", "UPTOWN", "SUBURBS-N", "SUBURBS-S"]

# City abbreviations used in zone IDs and order events
CITY_ABBREV = {
    "Houston":       "HOU",
    "Dallas":        "DAL",
    "Austin":        "AUS",
    "San Antonio":   "SAT",
    "Fort Worth":    "FTW",
    "El Paso":       "ELP",
    "Arlington":     "ARL",
    "Corpus Christi":"CRP",
    "Plano":         "PLN",
    "Lubbock":       "LBB",
}

# GPS delivery zone centers — one per city × zone (used by gps_generator)
GPS_DELIVERY_ZONES: dict = {}
_CITY_CENTERS = {
    "HOU": (29.7604, -95.3698),
    "DAL": (32.7767, -96.7970),
    "AUS": (30.2672, -97.7431),
    "SAT": (29.4241, -98.4936),
    "FTW": (32.7555, -97.3308),
    "ELP": (31.7619, -106.4850),
    "ARL": (32.7357, -97.1081),
    "CRP": (27.8006, -97.3964),
    "PLN": (33.0198, -96.6989),
    "LBB": (33.5779, -101.8552),
}
_ZONE_OFFSETS = {
    "DOWNTOWN":  (0.000,  0.000),
    "MIDTOWN":   (-0.015, -0.018),
    "UPTOWN":    (0.020,  0.010),
    "SUBURBS-N": (0.060,  0.005),
    "SUBURBS-S": (-0.060, -0.005),
}
for _abbrev, (_clat, _clon) in _CITY_CENTERS.items():
    for _zone, (_dlat, _dlon) in _ZONE_OFFSETS.items():
        GPS_DELIVERY_ZONES[f"{_abbrev}-{_zone}"] = {
            "center_lat": round(_clat + _dlat, 6),
            "center_lon": round(_clon + _dlon, 6),
        }
