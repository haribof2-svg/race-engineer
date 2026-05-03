
# ============================================================
# Race Engineer - Endurance Moto
# Version robuste avec Live + Simulation + stratégie + comparatif
# ============================================================

import json
import random
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

# ============================================================
# CONFIGURATION
# ============================================================

st.set_page_config(page_title="Race Engineer - Endurance Moto", layout="wide")

DEFAULT_LIVE_URL = "https://fimewc.live-frclassification.fr/r1.json"
DB_PATH = Path("race_engineer_history.sqlite")
CONFIG_PATH = Path("race_engineer_config.json")

DEFAULT_CONFIG = {
    "data_source": "Simulation",
    "live_url": DEFAULT_LIVE_URL,
    "my_bike": "96",
    "team_name": "LEGACY COMPETITION",
    "category": "PRD",
    "refresh_ms": 1000,
    "fuel_capacity_l": 24.0,
    "fuel_per_lap_l": 0.70,
    "fuel_safety_laps": 2,
    "front_tire_life_laps": 80,
    "rear_tire_life_laps": 55,
    "brake_life_laps": 220,
    "chain_check_interval_laps": 40,
    "simulation_speed": 1,
    "drivers": [
        {"name": "HUGOT Jonathan", "armband": "Rouge", "color": "#ff4b4b", "order": 1, "active": True},
        {"name": "Pilote 2", "armband": "Bleu", "color": "#4b8bff", "order": 2, "active": True},
        {"name": "Pilote 3", "armband": "Jaune", "color": "#ffd84b", "order": 3, "active": True},
    ],
}

SIM_BIKES = [
    {"num": "96", "cat": "PRD", "team": "LEGACY COMPETITION", "brand": "Yamaha", "tires": "Dunlop", "base": 103.2, "relay": 24, "pit": 155},
    {"num": "42", "cat": "PRD", "team": "GREENTEAM 42 LYCEE SAINTE CLAIRE", "brand": "Kawasaki", "tires": "Dunlop", "base": 103.7, "relay": 23, "pit": 160},
    {"num": "199", "cat": "PRD", "team": "ARTEC #199", "brand": "Kawasaki", "tires": "Dunlop", "base": 104.2, "relay": 25, "pit": 166},
    {"num": "222", "cat": "PRD", "team": "Team Supermoto Racing", "brand": "Yamaha", "tires": "Dunlop", "base": 104.6, "relay": 22, "pit": 185},
    {"num": "531", "cat": "PRD", "team": "Mana-au Competition", "brand": "Honda", "tires": "Dunlop", "base": 105.0, "relay": 26, "pit": 170},
    {"num": "16", "cat": "PRD", "team": "Team HTC Racing", "brand": "Yamaha", "tires": "Dunlop", "base": 104.0, "relay": 21, "pit": 162},
    {"num": "210", "cat": "PRD", "team": "Team Grip Attack", "brand": "Honda", "tires": "Dunlop", "base": 105.4, "relay": 24, "pit": 190},
    {"num": "13", "cat": "PRD", "team": "Flying Buffs M3 Racing", "brand": "BMW", "tires": "Dunlop", "base": 106.0, "relay": 23, "pit": 175},
]

PAGES = [
    "Dashboard",
    "Live Timing",
    "Notre moto",
    "Prochain arrêt",
    "Pilotes",
    "Concurrents",
    "Détail concurrent",
    "Comparatif 96 vs concurrent",
    "Stratégie",
    "Simulation",
    "Historique & exports",
    "Paramètres",
]

# ============================================================
# ETAT SESSION
# ============================================================

if "bike_state" not in st.session_state:
    st.session_state.bike_state = {}
if "relay_history" not in st.session_state:
    st.session_state.relay_history = []
if "config" not in st.session_state:
    st.session_state.config = None
if "planned_events" not in st.session_state:
    st.session_state.planned_events = []
if "sim_state" not in st.session_state:
    st.session_state.sim_state = None
if "last_payload" not in st.session_state:
    st.session_state.last_payload = None

# ============================================================
# OUTILS CONFIG
# ============================================================

def load_config():
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            cfg = DEFAULT_CONFIG.copy()
            cfg.update(loaded)
            return cfg
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config(config):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


if st.session_state.config is None:
    st.session_state.config = load_config()

CONFIG = st.session_state.config
MY_BIKE = str(CONFIG.get("my_bike", "96"))
DATA_SOURCE = CONFIG.get("data_source", "Simulation")
LIVE_URL = CONFIG.get("live_url", DEFAULT_LIVE_URL)

st_autorefresh(interval=int(CONFIG.get("refresh_ms", 1000)), key="race_engineer_refresh")

# ============================================================
# OUTILS GENERAUX
# ============================================================

def clean_text(value):
    if value is None:
        return ""
    value = str(value)
    value = re.sub(r"\{.*?\}", "", value)
    return value.strip()


def clean_rider(value):
    return clean_text(value).replace("*", "").strip()


def to_int(value):
    try:
        value = clean_text(value)
        value = value.replace("Lp.", "").replace("Lp", "").strip()
        if value in ["", "-"]:
            return None
        return int(float(value))
    except Exception:
        return None


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def parse_time_to_seconds(value):
    value = clean_text(value)
    if not value or value == "-":
        return None
    try:
        parts = value.split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        return float(value)
    except Exception:
        return None


def seconds_to_laptime(seconds):
    if seconds is None or pd.isna(seconds):
        return "-"
    seconds = float(seconds)
    if seconds >= 3600:
        hours = int(seconds // 3600)
        minutes = int((seconds - hours * 3600) // 60)
        rest = seconds - hours * 3600 - minutes * 60
        return f"{hours}:{minutes:02d}:{rest:06.3f}"
    minutes = int(seconds // 60)
    rest = seconds - minutes * 60
    return f"{minutes}:{rest:06.3f}"


def is_pit_in(value):
    return "Pit In" in clean_text(value)


def is_pit_out(value):
    return "Pit Out" in clean_text(value)


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def active_drivers():
    drivers = [d for d in CONFIG.get("drivers", []) if d.get("active", True)]
    drivers = sorted(drivers, key=lambda d: d.get("order", 99))
    if not drivers:
        drivers = DEFAULT_CONFIG["drivers"]
    return drivers


def active_driver_names():
    return [d.get("name", f"Pilote {idx + 1}") for idx, d in enumerate(active_drivers())]


def next_driver_name(current_rider):
    drivers = active_drivers()
    if not drivers:
        return "-"
    names = [d.get("name", "") for d in drivers]
    current_clean = clean_rider(current_rider).lower()
    current_index = None
    for idx, name in enumerate(names):
        low = name.lower()
        if current_clean and (low in current_clean or current_clean in low):
            current_index = idx
            break
    if current_index is None:
        return names[0]
    return names[(current_index + 1) % len(names)]

# ============================================================
# BASE SQLITE + MIGRATIONS
# ============================================================

def ensure_column(conn, table, column, column_type):
    existing = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
            source TEXT,
            payload TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relay_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            numero TEXT,
            categorie TEXT,
            team TEXT,
            pilote TEXT,
            relais INTEGER,
            tours_relais INTEGER,
            last_pit_time TEXT,
            tour_total INTEGER,
            total_pit INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lap_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            numero TEXT,
            categorie TEXT,
            team TEXT,
            pilote TEXT,
            tour_total INTEGER,
            lap_time TEXT,
            lap_seconds REAL,
            relay_laps INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS service_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            numero TEXT,
            lap INTEGER,
            action TEXT,
            driver_next TEXT,
            fuel_refilled INTEGER,
            front_tire_changed INTEGER,
            rear_tire_changed INTEGER,
            brake_changed INTEGER,
            chain_checked INTEGER,
            pit_time_seconds REAL,
            note TEXT
        )
    """)

    for table, cols in {
        "snapshots": [("source", "TEXT")],
        "relay_events": [("categorie", "TEXT"), ("team", "TEXT"), ("pilote", "TEXT"), ("total_pit", "INTEGER")],
        "lap_events": [("categorie", "TEXT"), ("team", "TEXT"), ("pilote", "TEXT"), ("relay_laps", "INTEGER")],
        "service_events": [
            ("numero", "TEXT"), ("lap", "INTEGER"), ("action", "TEXT"), ("driver_next", "TEXT"),
            ("fuel_refilled", "INTEGER"), ("front_tire_changed", "INTEGER"), ("rear_tire_changed", "INTEGER"),
            ("brake_changed", "INTEGER"), ("chain_checked", "INTEGER"), ("pit_time_seconds", "REAL"), ("note", "TEXT"),
        ],
    }.items():
        for column, column_type in cols:
            ensure_column(conn, table, column, column_type)

    conn.commit()
    return conn


def empty_df(columns):
    return pd.DataFrame(columns=columns)


def save_snapshot_to_db(data, source):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO snapshots (fetched_at, source, payload) VALUES (?, ?, ?)",
            (now_text(), source, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def save_relay_event_to_db(event):
    try:
        conn = db_connect()
        conn.execute("""
            INSERT INTO relay_events (
                timestamp, numero, categorie, team, pilote, relais,
                tours_relais, last_pit_time, tour_total, total_pit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["timestamp"], event["numero"], event["categorie"], event["team"], event["pilote"],
            event["relais"], event["tours_relais"], event["last_pit_time"], event["tour_total"], event["total_pit"],
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass


def save_lap_event_to_db(event):
    try:
        conn = db_connect()
        conn.execute("""
            INSERT INTO lap_events (
                timestamp, numero, categorie, team, pilote, tour_total,
                lap_time, lap_seconds, relay_laps
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["timestamp"], event["numero"], event["categorie"], event["team"], event["pilote"],
            event["tour_total"], event["lap_time"], event["lap_seconds"], event["relay_laps"],
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass


def save_service_event(event):
    conn = db_connect()
    conn.execute("""
        INSERT INTO service_events (
            timestamp, numero, lap, action, driver_next, fuel_refilled,
            front_tire_changed, rear_tire_changed, brake_changed, chain_checked,
            pit_time_seconds, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event["timestamp"], event["numero"], event["lap"], event["action"], event["driver_next"],
        int(event["fuel_refilled"]), int(event["front_tire_changed"]), int(event["rear_tire_changed"]),
        int(event["brake_changed"]), int(event["chain_checked"]), event["pit_time_seconds"], event["note"],
    ))
    conn.commit()
    conn.close()


def load_relay_events():
    columns = ["timestamp", "numero", "categorie", "team", "pilote", "relais", "tours_relais", "last_pit_time", "tour_total", "total_pit"]
    try:
        conn = db_connect()
        df = pd.read_sql_query(
            "SELECT timestamp, numero, categorie, team, pilote, relais, tours_relais, last_pit_time, tour_total, total_pit FROM relay_events ORDER BY id ASC",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return empty_df(columns)


def load_lap_events(numero=None):
    columns = ["timestamp", "numero", "categorie", "team", "pilote", "tour_total", "lap_time", "lap_seconds", "relay_laps"]
    try:
        conn = db_connect()
        if numero:
            df = pd.read_sql_query(
                "SELECT timestamp, numero, categorie, team, pilote, tour_total, lap_time, lap_seconds, relay_laps FROM lap_events WHERE numero = ? ORDER BY id ASC",
                conn,
                params=(str(numero),),
            )
        else:
            df = pd.read_sql_query(
                "SELECT timestamp, numero, categorie, team, pilote, tour_total, lap_time, lap_seconds, relay_laps FROM lap_events ORDER BY id ASC",
                conn,
            )
        conn.close()
        return df
    except Exception:
        return empty_df(columns)


def load_service_events(numero=None):
    columns = [
        "timestamp", "numero", "lap", "action", "driver_next", "fuel_refilled", "front_tire_changed",
        "rear_tire_changed", "brake_changed", "chain_checked", "pit_time_seconds", "note",
    ]
    try:
        conn = db_connect()
        if numero:
            df = pd.read_sql_query(
                "SELECT timestamp, numero, lap, action, driver_next, fuel_refilled, front_tire_changed, rear_tire_changed, brake_changed, chain_checked, pit_time_seconds, note FROM service_events WHERE numero = ? ORDER BY id ASC",
                conn,
                params=(str(numero),),
            )
        else:
            df = pd.read_sql_query(
                "SELECT timestamp, numero, lap, action, driver_next, fuel_refilled, front_tire_changed, rear_tire_changed, brake_changed, chain_checked, pit_time_seconds, note FROM service_events ORDER BY id ASC",
                conn,
            )
        conn.close()
        return df
    except Exception:
        return empty_df(columns)


def load_snapshots():
    try:
        conn = db_connect()
        df = pd.read_sql_query("SELECT id, fetched_at, source, payload FROM snapshots ORDER BY id ASC", conn)
        conn.close()
        return df
    except Exception:
        return empty_df(["id", "fetched_at", "source", "payload"])


def reset_database():
    conn = db_connect()
    conn.execute("DELETE FROM snapshots")
    conn.execute("DELETE FROM relay_events")
    conn.execute("DELETE FROM lap_events")
    conn.execute("DELETE FROM service_events")
    conn.commit()
    conn.close()
    st.session_state.bike_state = {}
    st.session_state.relay_history = []

# ============================================================
# DONNEES LIVE / SIMULATION
# ============================================================

def fetch_live_data():
    try:
        ts = int(datetime.now().timestamp() * 1000)
        response = requests.get(f"{LIVE_URL}?t={ts}", timeout=10)
        response.raise_for_status()
        return json.loads(response.content.decode("utf-8-sig"))
    except Exception as exc:
        st.error(f"Impossible de récupérer les données live : {exc}")
        return None


def payload_to_dataframe(data):
    names = [col["Texte"] for col in data["Colonnes"]]
    df = pd.DataFrame(data["Donnees"], columns=names)
    if "" in df.columns:
        df = df.rename(columns={"": "Etat"})
    return df


def init_simulation():
    drivers = active_driver_names()
    bikes = {}
    for spec in SIM_BIKES:
        start_laps = random.randint(0, 4)
        bikes[spec["num"]] = {
            **spec,
            "laps": start_laps,
            "last_pit": random.randint(1, 6),
            "total_pit": 0,
            "last_pit_time": "-",
            "total_pit_seconds": 0.0,
            "best_lap_seconds": None,
            "last_lap_seconds": spec["base"],
            "current_driver_index": 0,
            "current_driver": drivers[0] if spec["num"] == MY_BIKE else f"Pilote #{spec['num']}",
            "relay_target": max(8, int(random.gauss(spec["relay"], 2))),
            "pit_ticks": 0,
            "pending_pit_seconds": None,
            "status": "_TrackPassing",
            "last_lap_text": seconds_to_laptime(spec["base"]),
        }
    st.session_state.sim_state = {"started_at": now_text(), "tick": 0, "bikes": bikes}
    st.session_state.bike_state = {}
    st.session_state.relay_history = []


def reset_simulation(clear_db=False):
    init_simulation()
    if clear_db:
        reset_database()


def apply_manual_pit_to_simulation(lap, pit_seconds, driver_next):
    sim = st.session_state.sim_state
    if not sim:
        return
    bike = sim.get("bikes", {}).get(MY_BIKE)
    if not bike:
        return
    bike["last_pit"] = 0
    bike["total_pit"] += 1
    bike["last_pit_time"] = seconds_to_laptime(pit_seconds)
    bike["total_pit_seconds"] += pit_seconds
    bike["relay_target"] = max(8, int(random.gauss(bike["relay"], 2)))
    if driver_next:
        bike["current_driver"] = driver_next


def simulation_step():
    if st.session_state.sim_state is None:
        init_simulation()

    sim = st.session_state.sim_state
    sim["tick"] += int(CONFIG.get("simulation_speed", 1))
    drivers = active_driver_names()

    for num, bike in sim["bikes"].items():
        if bike["pit_ticks"] > 0:
            bike["last_pit"] = 0
            bike["last_lap_text"] = "{sortie-stand}Pit Out" if bike["pit_ticks"] == 1 else "{entree-stand}Pit In"
            bike["status"] = "_PitOut" if bike["pit_ticks"] == 1 else "_MaximumTime"
            bike["pit_ticks"] -= 1
            if bike["pit_ticks"] == 0:
                pit_seconds = bike["pending_pit_seconds"] or bike["pit"]
                bike["last_pit_time"] = seconds_to_laptime(pit_seconds)
                bike["total_pit_seconds"] += pit_seconds
                bike["pending_pit_seconds"] = None
                bike["relay_target"] = max(8, int(random.gauss(bike["relay"], 2)))
                if num == MY_BIKE:
                    bike["current_driver_index"] = (bike["current_driver_index"] + 1) % len(drivers)
                    bike["current_driver"] = drivers[bike["current_driver_index"]]
            continue

        degradation = max(0, bike["last_pit"] - 10) * 0.04
        traffic = random.choice([0, 0, 0, 0.15, 0.25, -0.10])
        noise = random.gauss(0, 0.45)
        lap_seconds = max(90.0, bike["base"] + degradation + traffic + noise)

        bike["laps"] += 1
        bike["last_pit"] += 1
        bike["last_lap_seconds"] = lap_seconds
        bike["last_lap_text"] = seconds_to_laptime(lap_seconds)
        bike["status"] = "_TrackPassing"

        if bike["best_lap_seconds"] is None or lap_seconds < bike["best_lap_seconds"]:
            bike["best_lap_seconds"] = lap_seconds

        if bike["last_pit"] >= bike["relay_target"]:
            bike["total_pit"] += 1
            pit_seconds = max(70.0, random.gauss(bike["pit"], 18))
            bike["pending_pit_seconds"] = pit_seconds
            bike["pit_ticks"] = max(2, int(pit_seconds / 60))
            bike["last_pit"] = 0
            bike["last_lap_text"] = "{entree-stand}Pit In"
            bike["status"] = "_MaximumTime"


def simulation_payload():
    simulation_step()
    sim = st.session_state.sim_state
    rows = []
    bikes_sorted = sorted(sim["bikes"].values(), key=lambda b: (-b["laps"], b["total_pit_seconds"]))
    leader_laps = bikes_sorted[0]["laps"] if bikes_sorted else 0

    for pos, bike in enumerate(bikes_sorted, start=1):
        cat_rows = [b for b in bikes_sorted if b["cat"] == bike["cat"]]
        cat_pos = cat_rows.index(bike) + 1
        gap_laps = leader_laps - bike["laps"]
        gap = "-" if gap_laps == 0 else f"{gap_laps} Lp."
        last_seconds = bike["last_lap_seconds"] or bike["base"]
        best = seconds_to_laptime(bike["best_lap_seconds"])
        ideal = seconds_to_laptime((bike["best_lap_seconds"] or bike["base"]) - 0.2)
        rows.append([
            bike["status"], str(pos), str(cat_pos), bike["num"], bike["cat"], "FRA", bike["team"],
            bike["current_driver"], str(bike["laps"]), bike["brand"], bike["tires"], gap,
            "-" if cat_pos == 1 else gap, gap, gap,
            seconds_to_laptime(last_seconds * 0.45), seconds_to_laptime(last_seconds * 0.28), seconds_to_laptime(last_seconds * 0.27),
            bike["last_lap_text"], best, ideal, str(bike["last_pit"]), bike["last_pit_time"],
            str(bike["total_pit"]), seconds_to_laptime(bike["total_pit_seconds"]),
        ])

    columns = [
        ("Image", ""), ("Position", "Pos."), ("PositionCategorie", "Cat.P"), ("Numero", "No."),
        ("Categorie", "Cat"), ("Sponsors", "Nat"), ("Equipe", "Team"), ("Nom", "Rider"),
        ("NbTour", "Laps"), ("Perso5", "Brand"), ("Perso4", "Tires"), ("Ecart1er", "Gap 1st"),
        ("Ecart1erCategorie", "Gap. with leader cat."), ("EcartPrec", "Gap.Prev"),
        ("EcartPrecCategorie", "Gap. with prev. cat."), ("Inter1", "S1"), ("Inter2", "S2"),
        ("Inter3", "S3"), ("TpsTour", "L. Lap"), ("MeilleurTour", "Best Lap"),
        ("TempsIdeal", "Ideal Lap Time"), ("TourDepuisStand", "Last Pit"),
        ("TpsStand", "Last Pit Time"), ("NbStand", "Total Pit"), ("TpsTotalStand", "Total pit time"),
    ]
    return {
        "Titre": "SIMULATION - Race Engineer",
        "HeureJourUTC": datetime.now(timezone.utc).isoformat(),
        "TempsEcoule": sim["tick"] * 1000,
        "TempsRestant": 0,
        "FrequenceActualisation": CONFIG.get("refresh_ms", 1000),
        "Filtre": ["Overall", "EWC", "EXP", "PRD", "SST"],
        "Colonnes": [{"Nom": name, "Texte": text, "Alignement": 1, "ModeImage": False, "ModeAffichage": 0} for name, text in columns],
        "Donnees": rows,
        "Messages": [f"Simulation tick {sim['tick']}"],
    }

# ============================================================
# DETECTION EVENEMENTS
# ============================================================

def process_relay_detection(df):
    required_cols = ["No.", "Cat", "Team", "Rider", "Laps", "Last Pit", "Last Pit Time", "Total Pit", "L. Lap"]
    for col in required_cols:
        if col not in df.columns:
            return

    timestamp = now_text()
    for _, row in df.iterrows():
        number = clean_text(row["No."])
        if not number:
            continue

        category = clean_text(row["Cat"])
        team = clean_text(row["Team"])
        rider = clean_rider(row["Rider"])
        laps = to_int(row["Laps"])
        last_pit = to_int(row["Last Pit"])
        last_pit_time = clean_text(row["Last Pit Time"])
        total_pit = to_int(row["Total Pit"])
        last_lap = clean_text(row["L. Lap"])
        last_lap_seconds = parse_time_to_seconds(last_lap)

        if number not in st.session_state.bike_state:
            st.session_state.bike_state[number] = {
                "previous_last_pit": last_pit,
                "previous_total_pit": total_pit,
                "last_recorded_total_pit": total_pit,
                "relay_number": 0,
                "last_recorded_lap": laps,
            }
            continue

        state = st.session_state.bike_state[number]
        previous_last_pit = state.get("previous_last_pit")
        previous_total_pit = state.get("previous_total_pit")
        last_recorded_total_pit = state.get("last_recorded_total_pit")
        last_recorded_lap = state.get("last_recorded_lap")

        stop_validated = False
        if total_pit is not None and previous_total_pit is not None and total_pit > previous_total_pit and total_pit != last_recorded_total_pit:
            stop_validated = True
        if last_pit == 0 and previous_last_pit is not None and previous_last_pit > 0 and (is_pit_in(last_lap) or is_pit_out(last_lap)) and total_pit != last_recorded_total_pit:
            stop_validated = True

        if stop_validated:
            relay_laps = previous_last_pit
            if relay_laps is not None and relay_laps > 0:
                state["relay_number"] = state.get("relay_number", 0) + 1
                event = {
                    "timestamp": timestamp,
                    "numero": number,
                    "categorie": category,
                    "team": team,
                    "pilote": rider,
                    "relais": state["relay_number"],
                    "tours_relais": relay_laps,
                    "last_pit_time": last_pit_time,
                    "tour_total": laps,
                    "total_pit": total_pit,
                }
                st.session_state.relay_history.append(event)
                save_relay_event_to_db(event)
                state["last_recorded_total_pit"] = total_pit

        if laps is not None and last_lap_seconds is not None:
            if last_recorded_lap is not None and laps > last_recorded_lap:
                lap_event = {
                    "timestamp": timestamp,
                    "numero": number,
                    "categorie": category,
                    "team": team,
                    "pilote": rider,
                    "tour_total": laps,
                    "lap_time": last_lap,
                    "lap_seconds": last_lap_seconds,
                    "relay_laps": last_pit,
                }
                save_lap_event_to_db(lap_event)
                state["last_recorded_lap"] = laps

        state["previous_last_pit"] = last_pit
        state["previous_total_pit"] = total_pit

# ============================================================
# CALCULS METIER
# ============================================================

def get_current_bike_row(df_live, numero=None):
    selected = str(numero or MY_BIKE)
    if df_live.empty or "No." not in df_live.columns:
        return None
    bike = df_live[df_live["No."].astype(str) == selected]
    if bike.empty:
        return None
    return bike.iloc[0]


def last_service_lap(events, field_name, current_lap, default_lap):
    if events.empty:
        return default_lap
    filtered = events[events[field_name].astype(str).isin(["1", "True", "true"])]
    if filtered.empty:
        return default_lap
    lap = to_int(filtered.iloc[-1]["lap"])
    if lap is None:
        return default_lap
    return min(lap, current_lap)


def calculate_bike_metrics(df_live, numero=None):
    row = get_current_bike_row(df_live, numero)
    if row is None:
        return None

    current_lap = to_int(row.get("Laps")) or 0
    live_last_pit = to_int(row.get("Last Pit")) or 0
    live_last_pit_lap = max(0, current_lap - live_last_pit)

    events = load_service_events(str(numero or MY_BIKE))
    fuel_lap = last_service_lap(events, "fuel_refilled", current_lap, live_last_pit_lap)
    front_lap = last_service_lap(events, "front_tire_changed", current_lap, live_last_pit_lap)
    rear_lap = last_service_lap(events, "rear_tire_changed", current_lap, live_last_pit_lap)
    brake_lap = last_service_lap(events, "brake_changed", current_lap, 0)
    chain_lap = last_service_lap(events, "chain_checked", current_lap, 0)

    laps_since_fuel = max(0, current_lap - fuel_lap)
    laps_since_front = max(0, current_lap - front_lap)
    laps_since_rear = max(0, current_lap - rear_lap)
    laps_since_brake = max(0, current_lap - brake_lap)
    laps_since_chain = max(0, current_lap - chain_lap)

    fuel_capacity = safe_float(CONFIG.get("fuel_capacity_l", 24.0), 24.0)
    fuel_per_lap = safe_float(CONFIG.get("fuel_per_lap_l", 0.70), 0.70)
    safety_laps = int(CONFIG.get("fuel_safety_laps", 2))
    front_life = max(1, int(CONFIG.get("front_tire_life_laps", 80)))
    rear_life = max(1, int(CONFIG.get("rear_tire_life_laps", 55)))
    brake_life = max(1, int(CONFIG.get("brake_life_laps", 220)))
    chain_interval = max(1, int(CONFIG.get("chain_check_interval_laps", 40)))

    fuel_remaining = max(fuel_capacity - laps_since_fuel * fuel_per_lap, 0)
    laps_remaining = int(fuel_remaining / fuel_per_lap) if fuel_per_lap > 0 else 0
    pit_window_min = max(laps_remaining - safety_laps, 0)

    front_pct = max(0, 100 - laps_since_front / front_life * 100)
    rear_pct = max(0, 100 - laps_since_rear / rear_life * 100)
    brake_pct = max(0, 100 - laps_since_brake / brake_life * 100)
    chain_due_in = max(0, chain_interval - laps_since_chain)

    avg_lap = lap_stats_for_bike(str(numero or MY_BIKE)).get("avg")
    if avg_lap is None:
        avg_lap = parse_time_to_seconds(row.get("L. Lap")) or 105.0
    next_stop_time = datetime.now() + timedelta(seconds=avg_lap * max(laps_remaining, 0))

    return {
        "row": row,
        "current_lap": current_lap,
        "fuel_remaining": fuel_remaining,
        "laps_remaining": laps_remaining,
        "pit_window": f"{pit_window_min}-{laps_remaining}",
        "front_tire_pct": front_pct,
        "rear_tire_pct": rear_pct,
        "brake_pct": brake_pct,
        "chain_due_in": chain_due_in,
        "laps_since_fuel": laps_since_fuel,
        "laps_since_front": laps_since_front,
        "laps_since_rear": laps_since_rear,
        "laps_since_brake": laps_since_brake,
        "laps_since_chain": laps_since_chain,
        "next_stop_lap": current_lap + laps_remaining,
        "next_stop_time": next_stop_time.strftime("%H:%M:%S"),
        "avg_lap_seconds": avg_lap,
    }


def status_text(percent, warning=35, critical=20, action_word="Changer"):
    if percent <= critical:
        return f"{action_word} urgent"
    if percent <= warning:
        return f"Prévoir {action_word.lower()}"
    return "OK"


def next_stop_table(metrics, df_live):
    row = metrics["row"]
    current_rider = clean_rider(row.get("Rider", ""))
    next_driver = next_driver_name(current_rider)
    chain_status = "Contrôler" if metrics["chain_due_in"] <= 5 else f"OK ({metrics['chain_due_in']} tours)"
    data = [{
        "Prochain pilote": next_driver,
        "Heure estimée": metrics["next_stop_time"],
        "N° tour course": metrics["next_stop_lap"],
        "Carburant": "Plein / reset conso",
        "Pneu AV": status_text(metrics["front_tire_pct"], action_word="changer AV"),
        "Pneu AR": status_text(metrics["rear_tire_pct"], action_word="changer AR"),
        "Plaquettes": status_text(metrics["brake_pct"], warning=40, critical=25, action_word="plaquettes"),
        "Chaîne": chain_status,
    }]
    return pd.DataFrame(data)


def lap_stats_for_bike(numero):
    df = load_lap_events(str(numero))
    if df.empty:
        return {}
    valid = df.dropna(subset=["lap_seconds"])
    if valid.empty:
        return {}
    return {
        "tours": len(valid),
        "best": valid["lap_seconds"].min(),
        "avg": valid["lap_seconds"].mean(),
        "median": valid["lap_seconds"].median(),
        "std": valid["lap_seconds"].std(),
        "pace5": valid.nsmallest(min(5, len(valid)), "lap_seconds")["lap_seconds"].mean(),
        "recent10": valid.tail(min(10, len(valid)))["lap_seconds"].mean(),
    }


def build_driver_summary():
    laps = load_lap_events(MY_BIKE)
    relays = load_relay_events()
    if laps.empty:
        return pd.DataFrame()
    rows = []
    for pilote, group in laps.groupby("pilote"):
        valid = group.dropna(subset=["lap_seconds"])
        if relays.empty:
            driver_relays = pd.DataFrame()
        else:
            driver_relays = relays[(relays["numero"].astype(str) == MY_BIKE) & (relays["pilote"] == pilote)]
        rows.append({
            "Pilote": pilote,
            "Tours effectués": len(valid),
            "Relais effectués": len(driver_relays),
            "Tours dernier relais": int(driver_relays["tours_relais"].iloc[-1]) if not driver_relays.empty else "-",
            "Tours moyens / relais": round(driver_relays["tours_relais"].mean(), 1) if not driver_relays.empty else "-",
            "Meilleur tour": seconds_to_laptime(valid["lap_seconds"].min() if not valid.empty else None),
            "Chrono moyen total": seconds_to_laptime(valid["lap_seconds"].mean() if not valid.empty else None),
            "Médiane": seconds_to_laptime(valid["lap_seconds"].median() if not valid.empty else None),
            "Régularité écart-type": round(valid["lap_seconds"].std(), 3) if len(valid) > 1 else "-",
            "Moyenne 10 derniers tours": seconds_to_laptime(valid.tail(min(10, len(valid)))["lap_seconds"].mean() if not valid.empty else None),
        })
    return pd.DataFrame(rows)


def relay_lap_avg(numero, start_lap, end_lap):
    laps = load_lap_events(str(numero))
    if laps.empty:
        return None
    part = laps[(laps["tour_total"] >= start_lap) & (laps["tour_total"] <= end_lap)].dropna(subset=["lap_seconds"])
    if part.empty:
        return None
    return part["lap_seconds"].mean()


def relay_lap_best(numero, start_lap, end_lap):
    laps = load_lap_events(str(numero))
    if laps.empty:
        return None
    part = laps[(laps["tour_total"] >= start_lap) & (laps["tour_total"] <= end_lap)].dropna(subset=["lap_seconds"])
    if part.empty:
        return None
    return part["lap_seconds"].min()


def build_relay_matrix(category_filter="PRD"):
    df_hist = load_relay_events()
    if df_hist.empty:
        return pd.DataFrame()
    if category_filter != "ALL":
        df_hist = df_hist[df_hist["categorie"] == category_filter]
    if df_hist.empty:
        return pd.DataFrame()

    matrix = df_hist.pivot_table(
        index=["numero", "team"],
        columns="relais",
        values="tours_relais",
        aggfunc="last"
    ).reset_index()

    matrix.columns = [f"Relais N°{c}" if isinstance(c, int) else c for c in matrix.columns]

    # Affichage propre : les tours par relais sont toujours des nombres entiers.
    # Le type nullable Int64 évite les 20.000000 tout en acceptant les cases vides.
    relay_cols = [c for c in matrix.columns if str(c).startswith("Relais N°")]
    for col in relay_cols:
        matrix[col] = pd.to_numeric(matrix[col], errors="coerce").astype("Int64")

    return matrix


def build_pit_time_matrix(category_filter="PRD"):
    df_hist = load_relay_events()
    if df_hist.empty:
        return pd.DataFrame()
    if category_filter != "ALL":
        df_hist = df_hist[df_hist["categorie"] == category_filter]
    if df_hist.empty:
        return pd.DataFrame()
    matrix = df_hist.pivot_table(index=["numero", "team"], columns="relais", values="last_pit_time", aggfunc="last").reset_index()
    matrix.columns = [f"Arrêt N°{c}" if isinstance(c, int) else c for c in matrix.columns]
    return matrix


def build_avg_lap_matrix(category_filter="PRD"):
    relays = load_relay_events()
    if relays.empty:
        return pd.DataFrame()
    if category_filter != "ALL":
        relays = relays[relays["categorie"] == category_filter]
    rows = []
    for _, relay in relays.iterrows():
        laps_count = to_int(relay["tours_relais"])
        end_lap = to_int(relay["tour_total"])
        if laps_count is None or end_lap is None:
            continue
        start_lap = max(1, end_lap - laps_count + 1)
        avg = relay_lap_avg(relay["numero"], start_lap, end_lap)
        rows.append({
            "numero": str(relay["numero"]),
            "team": relay["team"],
            "relais": relay["relais"],
            "avg_seconds": avg,
            "avg_text": seconds_to_laptime(avg),
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    matrix = df.pivot_table(index=["numero", "team"], columns="relais", values="avg_text", aggfunc="last").reset_index()
    matrix.columns = [f"Moy. N°{c}" if isinstance(c, int) else c for c in matrix.columns]
    return matrix


def build_my_relay_overview():
    relays = load_relay_events()
    if relays.empty:
        return pd.DataFrame()
    my_relays = relays[relays["numero"].astype(str) == MY_BIKE].copy()
    rows = []
    for _, relay in my_relays.iterrows():
        laps_count = to_int(relay["tours_relais"])
        end_lap = to_int(relay["tour_total"])
        if laps_count is None or end_lap is None:
            avg = None
            best = None
        else:
            start_lap = max(1, end_lap - laps_count + 1)
            avg = relay_lap_avg(MY_BIKE, start_lap, end_lap)
            best = relay_lap_best(MY_BIKE, start_lap, end_lap)
        rows.append({
            "Relais": relay["relais"],
            "Pilote": relay["pilote"],
            "Tours": relay["tours_relais"],
            "Moyenne relais": seconds_to_laptime(avg),
            "Best relais": seconds_to_laptime(best),
            "Pit time": relay["last_pit_time"],
            "Tour total fin relais": relay["tour_total"],
        })
    return pd.DataFrame(rows)


def build_comparison(my_num, comp_num):
    relays = load_relay_events()
    if relays.empty:
        return pd.DataFrame()
    my = relays[relays["numero"].astype(str) == str(my_num)].copy()
    cp = relays[relays["numero"].astype(str) == str(comp_num)].copy()
    if my.empty or cp.empty:
        return pd.DataFrame()
    max_relay = min(int(my["relais"].max()), int(cp["relais"].max()))
    rows = []
    cumulative = 0.0
    for relay_num in range(1, max_relay + 1):
        my_row = my[my["relais"] == relay_num]
        cp_row = cp[cp["relais"] == relay_num]
        if my_row.empty or cp_row.empty:
            continue
        my_row = my_row.iloc[-1]
        cp_row = cp_row.iloc[-1]
        my_laps = int(my_row["tours_relais"])
        cp_laps = int(cp_row["tours_relais"])
        my_end = to_int(my_row["tour_total"]) or 0
        cp_end = to_int(cp_row["tour_total"]) or 0
        my_start = max(1, my_end - my_laps + 1)
        cp_start = max(1, cp_end - cp_laps + 1)
        my_avg = relay_lap_avg(my_num, my_start, my_end)
        cp_avg = relay_lap_avg(comp_num, cp_start, cp_end)
        common_laps = min(my_laps, cp_laps)
        track_gain = None
        if my_avg is not None and cp_avg is not None:
            track_gain = (cp_avg - my_avg) * common_laps
        my_pit = parse_time_to_seconds(my_row["last_pit_time"])
        cp_pit = parse_time_to_seconds(cp_row["last_pit_time"])
        stand_gain = None
        if my_pit is not None and cp_pit is not None:
            stand_gain = cp_pit - my_pit
        total_gain = (track_gain or 0.0) + (stand_gain or 0.0)
        cumulative += total_gain
        rows.append({
            "Relais": relay_num,
            f"Tours {my_num}": my_laps,
            f"Tours {comp_num}": cp_laps,
            f"Moyenne {my_num}": seconds_to_laptime(my_avg),
            f"Moyenne {comp_num}": seconds_to_laptime(cp_avg),
            "Gain/perte piste (s)": round(track_gain, 3) if track_gain is not None else None,
            f"Arrêt {my_num}": my_row["last_pit_time"],
            f"Arrêt {comp_num}": cp_row["last_pit_time"],
            "Gain/perte stand (s)": round(stand_gain, 3) if stand_gain is not None else None,
            "Bilan relais (s)": round(total_gain, 3),
            "Cumul (s)": round(cumulative, 3),
        })
    return pd.DataFrame(rows)


def build_strategy_timeline(current_lap, future_laps=50):
    start_lap = max(current_lap - 10, 0)
    end_lap = current_lap + future_laps
    rows = []
    for lap in range(start_lap, end_lap + 1):
        events = [event["type"] for event in st.session_state.planned_events if int(event["lap"]) == lap]
        rows.append({
            "Tour": lap,
            "Zone": "Passé" if lap <= current_lap else "Futur",
            "Événement planifié": " / ".join(events),
        })
    return pd.DataFrame(rows)


def color_timeline(row):
    event = row.get("Événement planifié", "")
    zone = row.get("Zone", "")
    styles = [""] * len(row)
    if event:
        styles = ["background-color: #5c3b00; color: white"] * len(row)
    elif zone == "Futur":
        styles = ["background-color: #102033"] * len(row)
    return styles


def value_for_compare(value, mode):
    if mode == "time_text":
        return parse_time_to_seconds(value)
    return safe_float(value, None)


def style_matrix_vs_ours(df, prefix, mode, higher_is_bad=True):
    if df.empty or "numero" not in df.columns:
        return df
    my_rows = df[df["numero"].astype(str) == MY_BIKE]
    if my_rows.empty:
        return df
    my_row = my_rows.iloc[0]
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for idx, row in df.iterrows():
        if str(row.get("numero", "")) == MY_BIKE:
            styles.loc[idx, :] = "background-color: #10294d; font-weight: bold"
            continue
        for col in df.columns:
            if not str(col).startswith(prefix):
                continue
            own_value = value_for_compare(row[col], mode)
            my_value = value_for_compare(my_row[col], mode)
            if own_value is None or my_value is None:
                continue
            if higher_is_bad:
                if own_value > my_value:
                    styles.loc[idx, col] = "background-color: #5c1f1f; color: white"
                elif own_value < my_value:
                    styles.loc[idx, col] = "background-color: #143d1f; color: white"
            else:
                if own_value < my_value:
                    styles.loc[idx, col] = "background-color: #5c1f1f; color: white"
                elif own_value > my_value:
                    styles.loc[idx, col] = "background-color: #143d1f; color: white"
    try:
        styler = df.style.apply(lambda _: styles, axis=None)

        # Format d'affichage selon le type de tableau.
        # Pour les relais, on veut uniquement des nombres entiers, sans virgule.
        if mode == "number":
            format_cols = [c for c in df.columns if str(c).startswith(prefix)]
            styler = styler.format({c: "{:.0f}" for c in format_cols}, na_rep="")

        return styler
    except Exception:
        return df


def export_excel_bytes():
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        load_relay_events().to_excel(writer, sheet_name="Relais", index=False)
        load_lap_events().to_excel(writer, sheet_name="Tours", index=False)
        load_service_events().to_excel(writer, sheet_name="Interventions", index=False)
        pd.DataFrame(st.session_state.planned_events).to_excel(writer, sheet_name="Strategie", index=False)
        pd.DataFrame([CONFIG]).to_excel(writer, sheet_name="Parametres", index=False)
    output.seek(0)
    return output

# ============================================================
# ACTION MANUELLE PIT STOP
# ============================================================

def manual_pit_form(metrics, location_key):
    st.subheader("Forcer / valider un Pit Stop")
    st.caption("Permet de recaler la stratégie si la moto rentre plus tôt/plus tard, chute, arrêt exceptionnel, ou changement partiel de pneus.")
    current_lap = metrics["current_lap"] if metrics else 0
    current_rider = clean_rider(metrics["row"].get("Rider", "")) if metrics else ""
    default_next_driver = next_driver_name(current_rider)
    driver_options = active_driver_names()
    if default_next_driver not in driver_options:
        driver_options.insert(0, default_next_driver)

    with st.form(f"manual_pit_{location_key}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            pit_lap = st.number_input("Tour d'entrée au stand", min_value=0, value=int(current_lap), step=1, key=f"pit_lap_{location_key}")
        with col2:
            driver_next = st.selectbox("Pilote prochain relais", driver_options, index=driver_options.index(default_next_driver), key=f"driver_next_{location_key}")
        with col3:
            pit_time_seconds = st.number_input("Temps arrêt réel estimé (s)", min_value=0.0, value=150.0, step=1.0, key=f"pit_time_{location_key}")

        col4, col5, col6, col7, col8 = st.columns(5)
        with col4:
            fuel_refilled = st.checkbox("Carburant fait", value=True, key=f"fuel_{location_key}")
        with col5:
            front_tire_changed = st.checkbox("Pneu AV changé", value=False, key=f"front_{location_key}")
        with col6:
            rear_tire_changed = st.checkbox("Pneu AR changé", value=False, key=f"rear_{location_key}")
        with col7:
            brake_changed = st.checkbox("Plaquettes changées", value=False, key=f"brake_{location_key}")
        with col8:
            chain_checked = st.checkbox("Chaîne contrôlée", value=True, key=f"chain_{location_key}")

        note = st.text_input("Note", value="", key=f"note_{location_key}")
        submitted = st.form_submit_button("Valider Pit Stop / intervention")

    if submitted:
        event = {
            "timestamp": now_text(),
            "numero": MY_BIKE,
            "lap": int(pit_lap),
            "action": "PIT_STOP",
            "driver_next": driver_next,
            "fuel_refilled": fuel_refilled,
            "front_tire_changed": front_tire_changed,
            "rear_tire_changed": rear_tire_changed,
            "brake_changed": brake_changed,
            "chain_checked": chain_checked,
            "pit_time_seconds": float(pit_time_seconds),
            "note": note,
        }
        save_service_event(event)
        if DATA_SOURCE == "Simulation":
            apply_manual_pit_to_simulation(int(pit_lap), float(pit_time_seconds), driver_next)
        st.success("Pit stop / intervention validé et stratégie recalée.")
        st.rerun()

# ============================================================
# CHARGEMENT DONNEES
# ============================================================

if DATA_SOURCE == "Simulation":
    payload = simulation_payload()
else:
    payload = fetch_live_data()

if payload is not None:
    save_snapshot_to_db(payload, DATA_SOURCE)
    st.session_state.last_payload = payload
    df_live = payload_to_dataframe(payload)
    process_relay_detection(df_live)
else:
    df_live = pd.DataFrame()

# ============================================================
# MENU
# ============================================================

st.sidebar.title("Navigation")
page = st.sidebar.radio("Choisir une page", PAGES)
st.sidebar.caption(f"Source : {DATA_SOURCE}")
st.sidebar.caption(f"Moto suivie : {MY_BIKE}")

st.title("Race Engineer - Endurance Moto")

# ============================================================
# PAGES
# ============================================================

if page == "Dashboard":
    st.header("Tableau de bord")
    metrics = calculate_bike_metrics(df_live)
    if metrics is None:
        st.warning(f"Moto {MY_BIKE} non trouvée.")
    else:
        bike = metrics["row"]
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Position", bike.get("Pos.", "-"))
        col2.metric("Position catégorie", bike.get("Cat.P", "-"))
        col3.metric("Tours", bike.get("Laps", "-"))
        col4.metric("Dernier tour", clean_text(bike.get("L. Lap", "-")))
        col5.metric("Meilleur tour", bike.get("Best Lap", "-"))
        col6.metric("Prochain pit estimé", f"{metrics['laps_remaining']} tours")

        st.subheader("Prochain arrêt - feuille d'information")
        st.dataframe(next_stop_table(metrics, df_live), use_container_width=True)

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.subheader("Carburant estimé")
            ratio = metrics["fuel_remaining"] / safe_float(CONFIG.get("fuel_capacity_l", 24.0), 24.0)
            st.progress(min(max(ratio, 0), 1))
            st.write(f"{metrics['fuel_remaining']:.1f} L / {CONFIG.get('fuel_capacity_l', 24.0):.1f} L")
            st.write(f"Pit window : **{metrics['pit_window']} tours**")
        with col_b:
            st.subheader("Pneus estimés")
            st.progress(min(max(metrics["front_tire_pct"] / 100, 0), 1))
            st.write(f"Pneu AV : {metrics['front_tire_pct']:.0f}% — {metrics['laps_since_front']} tours")
            st.progress(min(max(metrics["rear_tire_pct"] / 100, 0), 1))
            st.write(f"Pneu AR : {metrics['rear_tire_pct']:.0f}% — {metrics['laps_since_rear']} tours")
        with col_c:
            st.subheader("Freins / chaîne")
            st.progress(min(max(metrics["brake_pct"] / 100, 0), 1))
            st.write(f"Plaquettes : {metrics['brake_pct']:.0f}% — {metrics['laps_since_brake']} tours")
            st.write(f"Chaîne : contrôle dans {metrics['chain_due_in']} tours")

        manual_pit_form(metrics, "dashboard")

elif page == "Live Timing":
    st.header("Live Timing")
    if df_live.empty:
        st.error("Aucune donnée disponible.")
    else:
        category = st.selectbox("Filtrer catégorie", ["ALL", "EWC", "SST", "PRD", "EXP"], key="live_category")
        display_df = df_live.copy()
        if category != "ALL":
            display_df = display_df[display_df["Cat"] == category]
        st.dataframe(display_df, use_container_width=True, height=650)
        st.write(f"Nombre de motos affichées : {len(display_df)}")

elif page == "Notre moto":
    st.header("Notre moto - vue globale")
    metrics = calculate_bike_metrics(df_live)
    if metrics is None:
        st.warning(f"Moto {MY_BIKE} non trouvée.")
    else:
        bike = metrics["row"]
        stats = lap_stats_for_bike(MY_BIKE)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Équipe", bike.get("Team", "-"))
        col2.metric("Pilote actuel", clean_rider(bike.get("Rider", "-")))
        col3.metric("Catégorie", bike.get("Cat", "-"))
        col4.metric("Tours", bike.get("Laps", "-"))
        col5, col6, col7, col8 = st.columns(4)
        col5.metric("Dernier tour", clean_text(bike.get("L. Lap", "-")))
        col6.metric("Best live", bike.get("Best Lap", "-"))
        col7.metric("Moyenne enregistrée", seconds_to_laptime(stats.get("avg")))
        col8.metric("Pace 5 meilleurs", seconds_to_laptime(stats.get("pace5")))

        st.subheader("Tableau global relais / pit de notre moto")
        overview = build_my_relay_overview()
        if overview.empty:
            st.info("Aucun relais complet enregistré pour l'instant.")
        else:
            st.dataframe(overview, use_container_width=True)

        st.subheader("Matrice type Excel - tours par relais")
        relay_matrix = build_relay_matrix(CONFIG.get("category", "PRD"))
        if not relay_matrix.empty:
            my_line = relay_matrix[relay_matrix["numero"].astype(str) == MY_BIKE]
            st.dataframe(my_line, use_container_width=True)
        else:
            st.info("Matrice relais vide pour l'instant.")

        st.subheader("Historique interventions validées")
        services = load_service_events(MY_BIKE)
        st.dataframe(services, use_container_width=True)

        st.subheader("Tours enregistrés")
        st.dataframe(load_lap_events(MY_BIKE), use_container_width=True, height=350)

elif page == "Prochain arrêt":
    st.header("Prochain arrêt")
    metrics = calculate_bike_metrics(df_live)
    if metrics is None:
        st.warning("Aucune donnée disponible pour notre moto.")
    else:
        st.dataframe(next_stop_table(metrics, df_live), use_container_width=True)
        manual_pit_form(metrics, "next_stop")
        st.subheader("Historique interventions")
        st.dataframe(load_service_events(MY_BIKE), use_container_width=True)

elif page == "Pilotes":
    st.header("Pilotes")
    current = get_current_bike_row(df_live)
    current_rider = clean_rider(current.get("Rider", "")) if current is not None else ""
    drivers = active_drivers()

    st.subheader("Statut pilotes configurés")
    if drivers:
        cards = st.columns(len(drivers))
        for idx, driver in enumerate(drivers):
            with cards[idx]:
                name = driver.get("name", "")
                color = driver.get("color", "#ffffff")
                lower_name = name.lower()
                lower_current = current_rider.lower()
                is_current = bool(lower_current and (lower_name in lower_current or lower_current in lower_name))
                status = "EN RELAIS" if is_current else "Disponible"
                st.markdown(
                    f"""
                    <div style="border:1px solid #333;border-radius:12px;padding:14px;background:#111827;">
                        <div style="font-size:20px;font-weight:700;"><span style="color:{color};">●</span> {name}</div>
                        <div>Brassard : <b>{driver.get('armband', '')}</b></div>
                        <div>Ordre : <b>{driver.get('order', '')}</b></div>
                        <div>Statut : <b>{status}</b></div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.subheader("Statistiques pilotes")
    summary = build_driver_summary()
    if summary.empty:
        st.info("Aucun tour pilote enregistré.")
    else:
        st.dataframe(summary, use_container_width=True)

    if PLOTLY_OK:
        laps = load_lap_events(MY_BIKE)
        if not laps.empty:
            st.subheader("Graphique temps au tour par pilote")
            fig = px.line(laps, x="tour_total", y="lap_seconds", color="pilote", markers=True, title="Temps au tour par pilote")
            fig.update_yaxes(title="Temps au tour (s)", autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)

elif page == "Concurrents":
    st.header("Concurrents - relais, pit time et rythme")
    category = st.selectbox("Catégorie à surveiller", ["PRD", "EWC", "SST", "EXP", "ALL"], key="competitor_category")

    st.subheader("Classement live filtré")
    if df_live.empty:
        st.warning("Pas de données live.")
    else:
        comp = df_live.copy()
        if category != "ALL":
            comp = comp[comp["Cat"] == category]
        cols = ["Pos.", "Cat.P", "No.", "Cat", "Team", "Rider", "Laps", "L. Lap", "Best Lap", "Last Pit", "Last Pit Time", "Total Pit", "Total pit time"]
        cols = [col for col in cols if col in comp.columns]
        st.dataframe(comp[cols], use_container_width=True, height=350)

    st.subheader("Nombre de tours par relais - comparaison avec notre moto")
    relay_matrix = build_relay_matrix(category)
    if relay_matrix.empty:
        st.info("Aucun relais détecté.")
    else:
        styled = style_matrix_vs_ours(relay_matrix, "Relais", "number", higher_is_bad=False)
        st.dataframe(styled, use_container_width=True)

    st.subheader("Pit time par arrêt - comparaison avec notre moto")
    pit_matrix = build_pit_time_matrix(category)
    if pit_matrix.empty:
        st.info("Aucun temps d'arrêt enregistré.")
    else:
        styled_pit = style_matrix_vs_ours(pit_matrix, "Arrêt", "time_text", higher_is_bad=False)
        st.dataframe(styled_pit, use_container_width=True)

    st.subheader("Temps au tour moyen par relais")
    avg_matrix = build_avg_lap_matrix(category)
    if avg_matrix.empty:
        st.info("Aucune moyenne par relais disponible.")
    else:
        styled_avg = style_matrix_vs_ours(avg_matrix, "Moy.", "time_text", higher_is_bad=False)
        st.dataframe(styled_avg, use_container_width=True)

elif page == "Détail concurrent":
    st.header("Détail concurrent")
    if df_live.empty:
        st.warning("Pas de données.")
    else:
        options = df_live[df_live["No."].astype(str) != MY_BIKE]["No."].astype(str).tolist()
        if not options:
            st.info("Aucun concurrent disponible.")
        else:
            selected = st.selectbox("Concurrent", options)
            row = get_current_bike_row(df_live, selected)
            if row is not None:
                st.dataframe(pd.DataFrame([row]), use_container_width=True)

            st.subheader("Relais")
            relays = load_relay_events()
            if relays.empty:
                st.info("Aucun relais enregistré.")
            else:
                st.dataframe(relays[relays["numero"].astype(str) == str(selected)], use_container_width=True)

            st.subheader("Stats tours")
            stats = lap_stats_for_bike(selected)
            readable = {}
            for key, value in stats.items():
                if key in ["best", "avg", "median", "pace5", "recent10"]:
                    readable[key] = seconds_to_laptime(value)
                else:
                    readable[key] = value
            st.json(readable)

            laps = load_lap_events(selected)
            if PLOTLY_OK and not laps.empty:
                fig = px.line(laps, x="tour_total", y="lap_seconds", color="pilote", markers=True, title=f"Temps au tour #{selected}")
                fig.update_yaxes(title="Temps (s)", autorange="reversed")
                st.plotly_chart(fig, use_container_width=True)

elif page == "Comparatif 96 vs concurrent":
    st.header(f"Comparatif {MY_BIKE} vs concurrent")
    if df_live.empty:
        st.warning("Pas de données.")
    else:
        options = df_live[df_live["No."].astype(str) != MY_BIKE]["No."].astype(str).tolist()
        if not options:
            st.info("Aucun concurrent disponible.")
        else:
            selected = st.selectbox("Concurrent à comparer", options)
            comparison = build_comparison(MY_BIKE, selected)
            if comparison.empty:
                st.info("Pas encore assez de relais enregistrés pour faire le comparatif.")
            else:
                st.dataframe(comparison, use_container_width=True)
                if PLOTLY_OK:
                    fig = go.Figure()
                    fig.add_bar(x=comparison["Relais"], y=comparison["Gain/perte piste (s)"], name="Piste")
                    fig.add_bar(x=comparison["Relais"], y=comparison["Gain/perte stand (s)"], name="Stand")
                    fig.add_scatter(x=comparison["Relais"], y=comparison["Cumul (s)"], name="Cumul", mode="lines+markers")
                    fig.update_layout(title="Gain/perte par relais", barmode="relative", xaxis_title="Relais", yaxis_title="Secondes")
                    st.plotly_chart(fig, use_container_width=True)

elif page == "Stratégie":
    st.header("Stratégie")
    metrics = calculate_bike_metrics(df_live)
    if metrics is None:
        st.warning("Pas assez de données pour calculer la stratégie.")
    else:
        bike = metrics["row"]
        current_lap = metrics["current_lap"]
        col1, col2, col3 = st.columns(3)
        col1.metric("Carburant restant estimé", f"{metrics['fuel_remaining']:.1f} L")
        col2.metric("Tours restants", metrics["laps_remaining"])
        col3.metric("Pit window", metrics["pit_window"])

        st.subheader("Programmer un événement stratégique")
        col_a, col_b, col_c = st.columns([2, 2, 1])
        with col_a:
            event_lap = st.number_input("Tour", min_value=current_lap, max_value=current_lap + 150, value=current_lap + max(metrics["laps_remaining"], 1), step=1)
        with col_b:
            event_type = st.selectbox("Type d'événement", ["PIT", "Changement pilote", "Changement pneus", "Safety Car", "FCY", "Freins", "Note"])
        with col_c:
            st.write("")
            st.write("")
            add_event = st.button("Ajouter")
        if add_event:
            st.session_state.planned_events.append({"lap": int(event_lap), "type": event_type, "created_at": now_text()})
            st.rerun()
        if st.button("Effacer les événements planifiés"):
            st.session_state.planned_events = []
            st.rerun()

        st.subheader("Timeline stratégique")
        timeline = build_strategy_timeline(current_lap, future_laps=50)
        st.dataframe(timeline.style.apply(color_timeline, axis=1), use_container_width=True, height=500)

        st.subheader("Événements planifiés")
        st.dataframe(pd.DataFrame(st.session_state.planned_events), use_container_width=True)

elif page == "Simulation":
    st.header("Mode simulation")
    st.write("Ce mode permet de tester les relais, pit stops, stats pilotes, comparatifs et exports hors course live.")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Réinitialiser simulation"):
            reset_simulation(clear_db=False)
            st.success("Simulation réinitialisée.")
            st.rerun()
    with col2:
        if st.button("Réinitialiser simulation + historique"):
            reset_simulation(clear_db=True)
            st.success("Simulation et historique réinitialisés.")
            st.rerun()
    with col3:
        st.metric("Source actuelle", DATA_SOURCE)
    if st.session_state.sim_state:
        st.json({
            "started_at": st.session_state.sim_state.get("started_at"),
            "tick": st.session_state.sim_state.get("tick"),
            "bikes": len(st.session_state.sim_state.get("bikes", {})),
        })
    st.subheader("Données simulées")
    st.dataframe(df_live, use_container_width=True, height=400)

elif page == "Historique & exports":
    st.header("Historique & exports")
    relays = load_relay_events()
    laps = load_lap_events()
    services = load_service_events()
    snapshots = load_snapshots()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Relais", len(relays))
    col2.metric("Tours", len(laps))
    col3.metric("Interventions", len(services))
    col4.metric("Snapshots", len(snapshots))

    st.subheader("Exports")
    st.download_button("Télécharger relais CSV", relays.to_csv(index=False).encode("utf-8-sig"), "relay_events.csv", "text/csv")
    st.download_button("Télécharger tours CSV", laps.to_csv(index=False).encode("utf-8-sig"), "lap_events.csv", "text/csv")
    st.download_button("Télécharger interventions CSV", services.to_csv(index=False).encode("utf-8-sig"), "service_events.csv", "text/csv")
    st.download_button("Télécharger Excel complet", export_excel_bytes(), "race_engineer_export.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with st.expander("Historique relais"):
        st.dataframe(relays, use_container_width=True)
    with st.expander("Historique tours"):
        st.dataframe(laps, use_container_width=True)
    with st.expander("Historique interventions"):
        st.dataframe(services, use_container_width=True)
    with st.expander("Snapshots"):
        if snapshots.empty:
            st.info("Aucun snapshot.")
        else:
            st.dataframe(snapshots[["id", "fetched_at", "source"]], use_container_width=True)

elif page == "Paramètres":
    st.header("Paramètres")
    with st.form("settings_form"):
        st.subheader("Source de données")
        source_options = ["Live", "Simulation"]
        source_index = 0 if CONFIG.get("data_source") == "Live" else 1
        data_source = st.selectbox("Source", source_options, index=source_index)
        live_url = st.text_input("URL live timing JSON", value=CONFIG.get("live_url", DEFAULT_LIVE_URL))
        simulation_speed = st.number_input("Vitesse simulation", min_value=1, max_value=10, value=int(CONFIG.get("simulation_speed", 1)), step=1)

        st.subheader("Paramètres équipe")
        my_bike = st.text_input("Numéro de notre moto", value=str(CONFIG.get("my_bike", "96")))
        team_name = st.text_input("Nom de l'équipe", value=CONFIG.get("team_name", "LEGACY COMPETITION"))
        category_options = ["PRD", "EWC", "SST", "EXP", "ALL"]
        category_value = CONFIG.get("category", "PRD")
        category_index = category_options.index(category_value) if category_value in category_options else 0
        category = st.selectbox("Catégorie", category_options, index=category_index)
        refresh_ms = st.number_input("Rafraîchissement live (millisecondes)", min_value=1000, max_value=30000, value=int(CONFIG.get("refresh_ms", 1000)), step=1000)

        st.subheader("Carburant / pneus / freins / chaîne")
        fuel_capacity_l = st.number_input("Capacité réservoir estimée (L)", value=float(CONFIG.get("fuel_capacity_l", 24.0)), step=0.5)
        fuel_per_lap_l = st.number_input("Consommation estimée (L/tour)", value=float(CONFIG.get("fuel_per_lap_l", 0.70)), step=0.01)
        fuel_safety_laps = st.number_input("Marge sécurité carburant (tours)", value=int(CONFIG.get("fuel_safety_laps", 2)), step=1)
        front_tire_life_laps = st.number_input("Durée estimée pneu AV (tours)", value=int(CONFIG.get("front_tire_life_laps", 80)), step=1)
        rear_tire_life_laps = st.number_input("Durée estimée pneu AR (tours)", value=int(CONFIG.get("rear_tire_life_laps", 55)), step=1)
        brake_life_laps = st.number_input("Durée estimée freins / plaquettes (tours)", value=int(CONFIG.get("brake_life_laps", 220)), step=1)
        chain_check_interval_laps = st.number_input("Intervalle contrôle chaîne (tours)", value=int(CONFIG.get("chain_check_interval_laps", 40)), step=1)

        st.subheader("Pilotes / brassards")
        current_drivers = CONFIG.get("drivers", DEFAULT_CONFIG["drivers"])
        driver_count = st.selectbox("Nombre de pilotes", [3, 4], index=0 if len(current_drivers) <= 3 else 1)
        while len(current_drivers) < driver_count:
            current_drivers.append({"name": f"Pilote {len(current_drivers) + 1}", "armband": "", "color": "#ffffff", "order": len(current_drivers) + 1, "active": True})
        new_drivers = []
        for i in range(driver_count):
            driver = current_drivers[i]
            st.markdown(f"### Pilote {i + 1}")
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
            with col1:
                name = st.text_input(f"Nom pilote {i + 1}", value=driver.get("name", ""), key=f"driver_name_{i}")
            with col2:
                armband = st.text_input(f"Brassard {i + 1}", value=driver.get("armband", ""), key=f"driver_armband_{i}")
            with col3:
                color = st.color_picker(f"Couleur {i + 1}", value=driver.get("color", "#ffffff"), key=f"driver_color_{i}")
            with col4:
                order = st.number_input(f"Ordre {i + 1}", min_value=1, max_value=4, value=int(driver.get("order", i + 1)), step=1, key=f"driver_order_{i}")
            active = st.checkbox(f"Pilote {i + 1} actif", value=bool(driver.get("active", True)), key=f"driver_active_{i}")
            new_drivers.append({"name": name, "armband": armband, "color": color, "order": int(order), "active": active})

        submitted = st.form_submit_button("Enregistrer les paramètres")

    if submitted:
        CONFIG.update({
            "data_source": data_source,
            "live_url": live_url,
            "simulation_speed": int(simulation_speed),
            "my_bike": str(my_bike),
            "team_name": team_name,
            "category": category,
            "refresh_ms": int(refresh_ms),
            "fuel_capacity_l": float(fuel_capacity_l),
            "fuel_per_lap_l": float(fuel_per_lap_l),
            "fuel_safety_laps": int(fuel_safety_laps),
            "front_tire_life_laps": int(front_tire_life_laps),
            "rear_tire_life_laps": int(rear_tire_life_laps),
            "brake_life_laps": int(brake_life_laps),
            "chain_check_interval_laps": int(chain_check_interval_laps),
            "drivers": sorted(new_drivers, key=lambda x: x["order"]),
        })
        st.session_state.config = CONFIG
        save_config(CONFIG)
        st.success("Paramètres enregistrés.")
        st.rerun()
