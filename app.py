import json
import math
import random
import re
import sqlite3
from datetime import datetime
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
    "simulation_speed": 1,
    "drivers": [
        {"name": "HUGOT Jonathan", "armband": "Rouge", "color": "#ff4b4b", "order": 1, "active": True},
        {"name": "Pilote 2", "armband": "Bleu", "color": "#4b8bff", "order": 2, "active": True},
        {"name": "Pilote 3", "armband": "Jaune", "color": "#ffd84b", "order": 3, "active": True},
    ],
}

SIM_BIKES = [
    {"num":"96","cat":"PRD","team":"LEGACY COMPETITION","brand":"Yamaha","tires":"Dunlop","base":103.2,"relay":24,"pit":155},
    {"num":"42","cat":"PRD","team":"GREENTEAM 42 LYCEE SAINTE CLAIRE","brand":"Kawasaki","tires":"Dunlop","base":103.7,"relay":23,"pit":160},
    {"num":"199","cat":"PRD","team":"ARTEC #199","brand":"Kawasaki","tires":"Dunlop","base":104.2,"relay":25,"pit":166},
    {"num":"222","cat":"PRD","team":"Team Supermoto Racing","brand":"Yamaha","tires":"Dunlop","base":104.6,"relay":22,"pit":185},
    {"num":"531","cat":"PRD","team":"Mana-au Competition","brand":"Honda","tires":"Dunlop","base":105.0,"relay":26,"pit":170},
    {"num":"16","cat":"PRD","team":"Team HTC Racing","brand":"Yamaha","tires":"Dunlop","base":104.0,"relay":21,"pit":162},
    {"num":"210","cat":"PRD","team":"Team Grip Attack","brand":"Honda","tires":"Dunlop","base":105.4,"relay":24,"pit":190},
    {"num":"13","cat":"PRD","team":"Flying Buffs M3 Racing","brand":"BMW","tires":"Dunlop","base":106.0,"relay":23,"pit":175},
]

for key, default in {
    "bike_state": {},
    "relay_history": [],
    "config": None,
    "planned_events": [],
    "sim_state": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

def load_config():
    if CONFIG_PATH.exists():
        try:
            cfg = DEFAULT_CONFIG.copy()
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
            return cfg
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()

def save_config(config):
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

if st.session_state.config is None:
    st.session_state.config = load_config()
CONFIG = st.session_state.config
MY_BIKE = str(CONFIG.get("my_bike", "96"))
DATA_SOURCE = CONFIG.get("data_source", "Simulation")
LIVE_URL = CONFIG.get("live_url", DEFAULT_LIVE_URL)
st_autorefresh(interval=int(CONFIG.get("refresh_ms", 1000)), key="race_engineer_refresh")

def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\{.*?\}", "", str(value)).strip()

def clean_rider(value):
    return clean_text(value).replace("*", "").strip()

def to_int(value):
    try:
        value = clean_text(value).replace("Lp.", "").replace("Lp", "").strip()
        if value in ["", "-"]:
            return None
        return int(float(value))
    except Exception:
        return None

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
        h = int(seconds // 3600); m = int((seconds - h*3600)//60); s = seconds - h*3600 - m*60
        return f"{h}:{m:02d}:{s:06.3f}"
    m = int(seconds // 60); s = seconds - m*60
    return f"{m}:{s:06.3f}"

def is_pit_in(value):
    return "Pit In" in clean_text(value)

def is_pit_out(value):
    return "Pit Out" in clean_text(value)

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default

def ensure_column(conn, table, column, column_type):
    existing = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def db_connect():
    conn = sqlite3.connect(DB_PATH)

    conn.execute("CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, fetched_at TEXT NOT NULL, source TEXT, payload TEXT NOT NULL)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relay_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, numero TEXT, categorie TEXT, team TEXT, pilote TEXT,
            relais INTEGER, tours_relais INTEGER, last_pit_time TEXT, tour_total INTEGER, total_pit INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lap_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, numero TEXT, categorie TEXT, team TEXT, pilote TEXT,
            tour_total INTEGER, lap_time TEXT, lap_seconds REAL, relay_laps INTEGER
        )
    """)

    # Migration automatique pour les bases créées avec d'anciennes versions.
    ensure_column(conn, "snapshots", "source", "TEXT")
    ensure_column(conn, "relay_events", "categorie", "TEXT")
    ensure_column(conn, "relay_events", "team", "TEXT")
    ensure_column(conn, "relay_events", "pilote", "TEXT")
    ensure_column(conn, "relay_events", "total_pit", "INTEGER")
    ensure_column(conn, "lap_events", "categorie", "TEXT")
    ensure_column(conn, "lap_events", "team", "TEXT")
    ensure_column(conn, "lap_events", "pilote", "TEXT")
    ensure_column(conn, "lap_events", "relay_laps", "INTEGER")

    conn.commit()
    return conn

def save_snapshot_to_db(data, source):
    try:
        conn = db_connect(); conn.execute("INSERT INTO snapshots (fetched_at, source, payload) VALUES (?, ?, ?)", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), source, json.dumps(data, ensure_ascii=False))); conn.commit(); conn.close()
    except Exception:
        pass

def save_relay_event_to_db(event):
    try:
        conn = db_connect(); conn.execute("""
            INSERT INTO relay_events (timestamp, numero, categorie, team, pilote, relais, tours_relais, last_pit_time, tour_total, total_pit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (event["timestamp"], event["numero"], event["categorie"], event["team"], event["pilote"], event["relais"], event["tours_relais"], event["last_pit_time"], event["tour_total"], event["total_pit"])); conn.commit(); conn.close()
    except Exception:
        pass

def save_lap_event_to_db(event):
    try:
        conn = db_connect(); conn.execute("""
            INSERT INTO lap_events (timestamp, numero, categorie, team, pilote, tour_total, lap_time, lap_seconds, relay_laps)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (event["timestamp"], event["numero"], event["categorie"], event["team"], event["pilote"], event["tour_total"], event["lap_time"], event["lap_seconds"], event["relay_laps"])); conn.commit(); conn.close()
    except Exception:
        pass

def load_relay_events():
    conn = db_connect(); df = pd.read_sql_query("SELECT timestamp, numero, categorie, team, pilote, relais, tours_relais, last_pit_time, tour_total, total_pit FROM relay_events ORDER BY id ASC", conn); conn.close(); return df

def load_lap_events(numero=None):
    conn = db_connect()
    if numero:
        df = pd.read_sql_query("SELECT timestamp, numero, categorie, team, pilote, tour_total, lap_time, lap_seconds, relay_laps FROM lap_events WHERE numero = ? ORDER BY id ASC", conn, params=(str(numero),))
    else:
        df = pd.read_sql_query("SELECT timestamp, numero, categorie, team, pilote, tour_total, lap_time, lap_seconds, relay_laps FROM lap_events ORDER BY id ASC", conn)
    conn.close(); return df

def load_snapshots():
    conn = db_connect(); df = pd.read_sql_query("SELECT id, fetched_at, source, payload FROM snapshots ORDER BY id ASC", conn); conn.close(); return df

def reset_database():
    conn = db_connect(); conn.execute("DELETE FROM snapshots"); conn.execute("DELETE FROM relay_events"); conn.execute("DELETE FROM lap_events"); conn.commit(); conn.close()
    st.session_state.bike_state = {}; st.session_state.relay_history = []

def fetch_live_data():
    try:
        ts = int(datetime.now().timestamp() * 1000)
        r = requests.get(f"{LIVE_URL}?t={ts}", timeout=10)
        r.raise_for_status()
        return json.loads(r.content.decode("utf-8-sig"))
    except Exception as e:
        st.error(f"Impossible de récupérer les données live : {e}")
        return None

def payload_to_dataframe(data):
    noms = [c["Texte"] for c in data["Colonnes"]]
    df = pd.DataFrame(data["Donnees"], columns=noms)
    if "" in df.columns:
        df = df.rename(columns={"": "Etat"})
    return df

def active_driver_names():
    drivers = [d for d in CONFIG.get("drivers", []) if d.get("active", True)]
    drivers = sorted(drivers, key=lambda d: d.get("order", 99))
    names = [d.get("name", f"Pilote {i+1}") for i, d in enumerate(drivers)]
    return names or ["Pilote 1", "Pilote 2", "Pilote 3"]

def init_simulation():
    drivers = active_driver_names(); bikes = {}
    for spec in SIM_BIKES:
        bikes[spec["num"]] = {**spec, "laps":0, "last_pit":random.randint(1, 8), "total_pit":0, "last_pit_time":"-", "total_pit_seconds":0.0, "best_lap_seconds":None, "last_lap_seconds":spec["base"], "current_driver_index":0, "current_driver":drivers[0] if spec["num"] == MY_BIKE else f"Pilote #{spec['num']}", "relay_target":max(8, int(random.gauss(spec["relay"], 2))), "pit_ticks":0, "pending_pit_seconds":None, "status":"_TrackPassing", "last_lap_text":seconds_to_laptime(spec["base"])}
    st.session_state.sim_state = {"started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "tick": 0, "bikes": bikes}
    st.session_state.bike_state = {}; st.session_state.relay_history = []

def reset_simulation(clear_db=False):
    init_simulation()
    if clear_db:
        reset_database()

def simulation_step():
    if st.session_state.sim_state is None:
        init_simulation()
    sim = st.session_state.sim_state; sim["tick"] += int(CONFIG.get("simulation_speed", 1)); drivers = active_driver_names()
    for num, b in sim["bikes"].items():
        if b["pit_ticks"] > 0:
            b["status"] = "_PitOut" if b["pit_ticks"] == 1 else ""
            b["last_lap_text"] = "{sortie-stand}Pit Out" if b["pit_ticks"] == 1 else "{entree-stand}Pit In"
            b["last_pit"] = 0; b["pit_ticks"] -= 1
            if b["pit_ticks"] == 0:
                b["last_pit_time"] = seconds_to_laptime(b["pending_pit_seconds"])
                b["total_pit_seconds"] += b["pending_pit_seconds"] or 0
                b["pending_pit_seconds"] = None
                b["relay_target"] = max(8, int(random.gauss(b["relay"], 2)))
                if num == MY_BIKE:
                    b["current_driver_index"] = (b["current_driver_index"] + 1) % len(drivers)
                    b["current_driver"] = drivers[b["current_driver_index"]]
            continue
        degradation = max(0, b["last_pit"] - 10) * 0.04
        lap_seconds = max(90, b["base"] + degradation + random.gauss(0, 0.45) + random.choice([0,0,0,0.15,0.25,-0.10]))
        b["laps"] += 1; b["last_pit"] += 1; b["last_lap_seconds"] = lap_seconds; b["last_lap_text"] = seconds_to_laptime(lap_seconds); b["status"] = "_TrackPassing"
        if b["best_lap_seconds"] is None or lap_seconds < b["best_lap_seconds"]:
            b["best_lap_seconds"] = lap_seconds
        if b["last_pit"] >= b["relay_target"]:
            b["total_pit"] += 1
            pit_seconds = max(70, random.gauss(b["pit"], 18))
            b["pending_pit_seconds"] = pit_seconds; b["pit_ticks"] = max(2, int(pit_seconds / 60)); b["last_pit"] = 0; b["last_lap_text"] = "{entree-stand}Pit In"; b["status"] = "_MaximumTime"

def simulation_payload():
    simulation_step(); sim = st.session_state.sim_state; rows = []
    bikes_sorted = sorted(sim["bikes"].values(), key=lambda x: (-x["laps"], x["total_pit_seconds"]))
    leader_laps = bikes_sorted[0]["laps"] if bikes_sorted else 0
    for pos, b in enumerate(bikes_sorted, start=1):
        cat_rows = [x for x in bikes_sorted if x["cat"] == b["cat"]]; cat_pos = cat_rows.index(b) + 1; gap_laps = leader_laps - b["laps"]; gap = "-" if gap_laps == 0 else f"{gap_laps} Lp."; best = seconds_to_laptime(b["best_lap_seconds"]); ideal = seconds_to_laptime((b["best_lap_seconds"] or b["base"]) - 0.2)
        rows.append([b["status"], str(pos), str(cat_pos), b["num"], b["cat"], "FRA", b["team"], b["current_driver"], str(b["laps"]), b["brand"], b["tires"], gap, "-" if cat_pos == 1 else gap, gap, gap, seconds_to_laptime((b["last_lap_seconds"] or b["base"]) * 0.45), seconds_to_laptime((b["last_lap_seconds"] or b["base"]) * 0.28), seconds_to_laptime((b["last_lap_seconds"] or b["base"]) * 0.27), b["last_lap_text"], best, ideal, str(b["last_pit"]), b["last_pit_time"], str(b["total_pit"]), seconds_to_laptime(b["total_pit_seconds"])])
    cols = [("Image",""),("Position","Pos."),("PositionCategorie","Cat.P"),("Numero","No."),("Categorie","Cat"),("Sponsors","Nat"),("Equipe","Team"),("Nom","Rider"),("NbTour","Laps"),("Perso5","Brand"),("Perso4","Tires"),("Ecart1er","Gap 1st"),("Ecart1erCategorie","Gap. with leader cat."),("EcartPrec","Gap.Prev"),("EcartPrecCategorie","Gap. with prev. cat."),("Inter1","S1"),("Inter2","S2"),("Inter3","S3"),("TpsTour","L. Lap"),("MeilleurTour","Best Lap"),("TempsIdeal","Ideal Lap Time"),("TourDepuisStand","Last Pit"),("TpsStand","Last Pit Time"),("NbStand","Total Pit"),("TpsTotalStand","Total pit time")]
    return {"Titre":"SIMULATION - Race Engineer", "HeureJourUTC":datetime.utcnow().isoformat()+"Z", "TempsEcoule":sim["tick"]*1000, "TempsRestant":0, "FrequenceActualisation":CONFIG.get("refresh_ms",1000), "Filtre":["Overall","EWC","EXP","PRD","SST"], "Colonnes":[{"Nom":n,"Texte":t,"Alignement":1,"ModeImage":False,"ModeAffichage":0} for n,t in cols], "Donnees":rows, "Messages":[f"Simulation tick {sim['tick']}"]}

def process_relay_detection(df):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); required = ["No.","Cat","Team","Rider","Laps","Last Pit","Last Pit Time","Total Pit","L. Lap"]
    if any(c not in df.columns for c in required): return
    for _, row in df.iterrows():
        number = clean_text(row["No."]); category = clean_text(row["Cat"]); team = clean_text(row["Team"]); rider = clean_rider(row["Rider"]); laps = to_int(row["Laps"]); last_pit = to_int(row["Last Pit"]); last_pit_time = clean_text(row["Last Pit Time"]); total_pit = to_int(row["Total Pit"]); last_lap = clean_text(row["L. Lap"]); last_lap_seconds = parse_time_to_seconds(last_lap)
        if not number: continue
        if number not in st.session_state.bike_state:
            st.session_state.bike_state[number] = {"previous_last_pit": last_pit, "previous_total_pit": total_pit, "last_recorded_total_pit": total_pit, "relay_number": 0, "previous_laps": laps, "last_recorded_lap": laps}
            continue
        state = st.session_state.bike_state[number]; prev_lp = state.get("previous_last_pit"); prev_tp = state.get("previous_total_pit"); last_rec_tp = state.get("last_recorded_total_pit"); last_rec_lap = state.get("last_recorded_lap")
        stop_validated = total_pit is not None and prev_tp is not None and total_pit > prev_tp and total_pit != last_rec_tp
        if last_pit == 0 and prev_lp is not None and prev_lp > 0 and (is_pit_in(last_lap) or is_pit_out(last_lap)) and total_pit != last_rec_tp:
            stop_validated = True
        if stop_validated and prev_lp is not None and prev_lp > 0:
            state["relay_number"] = state.get("relay_number", 0) + 1
            event = {"timestamp": now, "numero": number, "categorie": category, "team": team, "pilote": rider, "relais": state["relay_number"], "tours_relais": prev_lp, "last_pit_time": last_pit_time, "tour_total": laps, "total_pit": total_pit}
            st.session_state.relay_history.append(event); save_relay_event_to_db(event); state["last_recorded_total_pit"] = total_pit
        if laps is not None and last_lap_seconds is not None and last_rec_lap is not None and laps > last_rec_lap:
            save_lap_event_to_db({"timestamp": now, "numero": number, "categorie": category, "team": team, "pilote": rider, "tour_total": laps, "lap_time": last_lap, "lap_seconds": last_lap_seconds, "relay_laps": last_pit}); state["last_recorded_lap"] = laps
        state["previous_last_pit"] = last_pit; state["previous_total_pit"] = total_pit; state["previous_laps"] = laps

def get_current_bike_row(df_live, numero=None):
    numero = str(numero or MY_BIKE)
    if df_live.empty or "No." not in df_live.columns: return None
    bike = df_live[df_live["No."].astype(str) == numero]
    return None if bike.empty else bike.iloc[0]

def calculate_bike_metrics(df_live, numero=None):
    row = get_current_bike_row(df_live, numero)
    if row is None: return None
    last_pit = to_int(row.get("Last Pit")); fuel_capacity = safe_float(CONFIG.get("fuel_capacity_l", 24.0), 24.0); fuel_per_lap = safe_float(CONFIG.get("fuel_per_lap_l", 0.70), 0.70); safety_laps = int(CONFIG.get("fuel_safety_laps", 2)); fuel_remaining = max(fuel_capacity - (last_pit or 0)*fuel_per_lap, 0); laps_remaining = int(fuel_remaining / fuel_per_lap) if fuel_per_lap > 0 else 0; pit_window_min = max(laps_remaining - safety_laps, 0)
    front_life=max(1,int(CONFIG.get("front_tire_life_laps",80))); rear_life=max(1,int(CONFIG.get("rear_tire_life_laps",55))); brake_life=max(1,int(CONFIG.get("brake_life_laps",220))); tire_laps=last_pit or 0
    return {"row": row, "fuel_remaining": fuel_remaining, "laps_remaining": laps_remaining, "pit_window": f"{pit_window_min}-{laps_remaining}", "front_tire_pct": max(0,100-tire_laps/front_life*100), "rear_tire_pct": max(0,100-tire_laps/rear_life*100), "brake_pct": max(0,100-(to_int(row.get("Laps")) or 0)/brake_life*100)}

def build_relay_matrix(category_filter="PRD"):
    df=load_relay_events();
    if df.empty: return pd.DataFrame()
    if category_filter!="ALL": df=df[df["categorie"]==category_filter]
    if df.empty: return pd.DataFrame()
    m=df.pivot_table(index=["numero","team"],columns="relais",values="tours_relais",aggfunc="last").reset_index(); m.columns=[f"Relais N°{c}" if isinstance(c,int) else c for c in m.columns]; return m

def build_pit_time_matrix(category_filter="PRD"):
    df=load_relay_events();
    if df.empty: return pd.DataFrame()
    if category_filter!="ALL": df=df[df["categorie"]==category_filter]
    if df.empty: return pd.DataFrame()
    m=df.pivot_table(index=["numero","team"],columns="relais",values="last_pit_time",aggfunc="last").reset_index(); m.columns=[f"Arrêt N°{c}" if isinstance(c,int) else c for c in m.columns]; return m

def lap_stats_for_bike(numero):
    df=load_lap_events(str(numero));
    if df.empty: return {}
    valid=df.dropna(subset=["lap_seconds"])
    if valid.empty: return {}
    return {"tours":len(valid),"best":valid["lap_seconds"].min(),"avg":valid["lap_seconds"].mean(),"median":valid["lap_seconds"].median(),"std":valid["lap_seconds"].std(),"pace5":valid.nsmallest(min(5,len(valid)),"lap_seconds")["lap_seconds"].mean(),"recent10":valid.tail(min(10,len(valid)))["lap_seconds"].mean()}

def build_driver_summary():
    laps=load_lap_events(MY_BIKE); relays=load_relay_events()
    if laps.empty: return pd.DataFrame()
    rows=[]
    for pilote, group in laps.groupby("pilote"):
        valid=group.dropna(subset=["lap_seconds"]); driver_relays=relays[(relays["numero"].astype(str)==MY_BIKE)&(relays["pilote"]==pilote)] if not relays.empty else pd.DataFrame()
        rows.append({"Pilote":pilote,"Tours effectués":len(valid),"Relais effectués":len(driver_relays),"Tours dernier relais":int(driver_relays["tours_relais"].iloc[-1]) if not driver_relays.empty else "-","Tours moyens / relais":round(driver_relays["tours_relais"].mean(),1) if not driver_relays.empty else "-","Meilleur tour":seconds_to_laptime(valid["lap_seconds"].min() if not valid.empty else None),"Chrono moyen total":seconds_to_laptime(valid["lap_seconds"].mean() if not valid.empty else None),"Médiane":seconds_to_laptime(valid["lap_seconds"].median() if not valid.empty else None),"Régularité écart-type":round(valid["lap_seconds"].std(),3) if len(valid)>1 else "-","Moyenne 10 derniers tours":seconds_to_laptime(valid.tail(min(10,len(valid)))["lap_seconds"].mean() if not valid.empty else None)})
    return pd.DataFrame(rows)

def relay_lap_avg(numero, start_lap, end_lap):
    laps=load_lap_events(str(numero)); part=laps[(laps["tour_total"]>=start_lap)&(laps["tour_total"]<=end_lap)].dropna(subset=["lap_seconds"]) if not laps.empty else pd.DataFrame()
    return None if part.empty else part["lap_seconds"].mean()

def build_comparison(my_num, comp_num):
    relays=load_relay_events()
    if relays.empty: return pd.DataFrame()
    my=relays[relays["numero"].astype(str)==str(my_num)].copy(); cp=relays[relays["numero"].astype(str)==str(comp_num)].copy()
    if my.empty or cp.empty: return pd.DataFrame()
    max_relay=min(my["relais"].max(), cp["relais"].max()); rows=[]; cumulative=0.0
    for relay in range(1,int(max_relay)+1):
        mr=my[my["relais"]==relay]; cr=cp[cp["relais"]==relay]
        if mr.empty or cr.empty: continue
        mr=mr.iloc[-1]; cr=cr.iloc[-1]; my_laps=int(mr["tours_relais"]); cp_laps=int(cr["tours_relais"]); my_end=int(mr["tour_total"] or 0); cp_end=int(cr["tour_total"] or 0); my_avg=relay_lap_avg(my_num,max(1,my_end-my_laps+1),my_end); cp_avg=relay_lap_avg(comp_num,max(1,cp_end-cp_laps+1),cp_end); common=min(my_laps,cp_laps); track_gain=(cp_avg-my_avg)*common if my_avg is not None and cp_avg is not None else None; my_pit=parse_time_to_seconds(mr["last_pit_time"]); cp_pit=parse_time_to_seconds(cr["last_pit_time"]); stand_gain=cp_pit-my_pit if my_pit is not None and cp_pit is not None else None; total_gain=(track_gain or 0)+(stand_gain or 0); cumulative+=total_gain
        rows.append({"Relais":relay,f"Tours {my_num}":my_laps,f"Tours {comp_num}":cp_laps,f"Moyenne {my_num}":seconds_to_laptime(my_avg),f"Moyenne {comp_num}":seconds_to_laptime(cp_avg),"Gain/perte piste (s)":round(track_gain,3) if track_gain is not None else None,f"Arrêt {my_num}":mr["last_pit_time"],f"Arrêt {comp_num}":cr["last_pit_time"],"Gain/perte stand (s)":round(stand_gain,3) if stand_gain is not None else None,"Bilan relais (s)":round(total_gain,3),"Cumul (s)":round(cumulative,3)})
    return pd.DataFrame(rows)

def build_strategy_timeline(current_lap, future_laps=50):
    rows=[]; planned=st.session_state.get("planned_events",[])
    for lap in range(max(current_lap-10,0), current_lap+future_laps+1):
        events=[e["type"] for e in planned if int(e["lap"])==lap]
        rows.append({"Tour":lap,"Zone":"Passé" if lap<=current_lap else "Futur","Événement planifié":" / ".join(events)})
    return pd.DataFrame(rows)

def color_timeline(row):
    if row.get("Événement planifié",""): return ["background-color:#5c3b00;color:white"]*len(row)
    if row.get("Zone","")=="Futur": return ["background-color:#102033"]*len(row)
    return [""]*len(row)

def export_excel_bytes():
    output=BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        load_relay_events().to_excel(writer,sheet_name="Relais",index=False); load_lap_events().to_excel(writer,sheet_name="Tours",index=False); pd.DataFrame(st.session_state.get("planned_events",[])).to_excel(writer,sheet_name="Strategie",index=False); pd.DataFrame([CONFIG]).to_excel(writer,sheet_name="Parametres",index=False)
    output.seek(0); return output

payload = simulation_payload() if DATA_SOURCE == "Simulation" else fetch_live_data()
if payload is not None:
    save_snapshot_to_db(payload, DATA_SOURCE); df_live=payload_to_dataframe(payload); process_relay_detection(df_live)
else:
    df_live=pd.DataFrame()

st.sidebar.title("Navigation")
page=st.sidebar.radio("Choisir une page",["Dashboard","Live Timing","Notre moto","Pilotes","Concurrents","Détail concurrent","Comparatif 96 vs concurrent","Stratégie","Simulation","Historique & exports","Paramètres"])
st.sidebar.caption(f"Source : {DATA_SOURCE}"); st.sidebar.caption(f"Moto suivie : {MY_BIKE}")
st.title("Race Engineer - Endurance Moto")

if page=="Dashboard":
    st.header("Tableau de bord"); metrics=calculate_bike_metrics(df_live)
    if metrics is None: st.warning(f"Moto {MY_BIKE} non trouvée.")
    else:
        b=metrics["row"]; c=st.columns(6); c[0].metric("Position",b.get("Pos.","-")); c[1].metric("Position catégorie",b.get("Cat.P","-")); c[2].metric("Tours",b.get("Laps","-")); c[3].metric("Dernier tour",clean_text(b.get("L. Lap","-"))); c[4].metric("Meilleur tour",b.get("Best Lap","-")); c[5].metric("Prochain pit estimé",f"{metrics['laps_remaining']} tours")
        a,bcol,ccol=st.columns(3)
        with a: st.subheader("Carburant estimé"); st.progress(min(max(metrics["fuel_remaining"]/safe_float(CONFIG.get("fuel_capacity_l",24.0),24.0),0),1)); st.write(f"{metrics['fuel_remaining']:.1f} L / {CONFIG.get('fuel_capacity_l',24.0):.1f} L"); st.write(f"Pit window : **{metrics['pit_window']} tours**")
        with bcol: st.subheader("Pneus estimés"); st.progress(min(max(metrics["front_tire_pct"]/100,0),1)); st.write(f"Avant : {metrics['front_tire_pct']:.0f}%"); st.progress(min(max(metrics["rear_tire_pct"]/100,0),1)); st.write(f"Arrière : {metrics['rear_tire_pct']:.0f}%")
        with ccol: st.subheader("Freins estimés"); st.progress(min(max(metrics["brake_pct"]/100,0),1)); st.write(f"Freins : {metrics['brake_pct']:.0f}%")
        st.subheader("Notre moto"); st.dataframe(pd.DataFrame([metrics["row"]]),use_container_width=True)

elif page=="Live Timing":
    st.header("Live Timing"); cat=st.selectbox("Filtrer catégorie",["ALL","EWC","SST","PRD","EXP"],key="live_category"); d=df_live.copy(); d=d if cat=="ALL" else d[d["Cat"]==cat]; st.dataframe(d,use_container_width=True,height=650); st.write(f"Nombre de motos affichées : {len(d)}")

elif page=="Notre moto":
    st.header("Notre moto"); metrics=calculate_bike_metrics(df_live)
    if metrics is None: st.warning(f"Moto {MY_BIKE} non trouvée.")
    else:
        b=metrics["row"]; stats=lap_stats_for_bike(MY_BIKE); c=st.columns(4); c[0].metric("Équipe",b.get("Team","-")); c[1].metric("Pilote actuel",clean_text(b.get("Rider","-"))); c[2].metric("Catégorie",b.get("Cat","-")); c[3].metric("Tours",b.get("Laps","-")); d=st.columns(4); d[0].metric("Dernier tour",clean_text(b.get("L. Lap","-"))); d[1].metric("Meilleur tour live",b.get("Best Lap","-")); d[2].metric("Moyenne enregistrée",seconds_to_laptime(stats.get("avg"))); d[3].metric("Pace 5 meilleurs",seconds_to_laptime(stats.get("pace5")))
        st.subheader("Relais de notre moto"); rel=load_relay_events(); st.dataframe(rel[rel["numero"].astype(str)==MY_BIKE] if not rel.empty else pd.DataFrame(),use_container_width=True)
        st.subheader("Tours enregistrés"); st.dataframe(load_lap_events(MY_BIKE),use_container_width=True,height=350)

elif page=="Pilotes":
    st.header("Pilotes"); current=get_current_bike_row(df_live); current_rider=clean_rider(current.get("Rider","")) if current is not None else ""; drivers=sorted(CONFIG.get("drivers",[]),key=lambda x:x.get("order",99)); st.subheader("Statut pilotes configurés")
    if drivers:
        cards=st.columns(len(drivers))
        for i,driver in enumerate(drivers):
            with cards[i]:
                name=driver.get("name",""); color=driver.get("color","#ffffff"); is_current=bool(current_rider and (name.lower() in current_rider.lower() or current_rider.lower() in name.lower()))
                st.markdown(f"""<div style='border:1px solid #333;border-radius:12px;padding:14px;background:#111827;'><div style='font-size:20px;font-weight:700;'><span style='color:{color};'>●</span> {name}</div><div>Brassard : <b>{driver.get('armband','')}</b></div><div>Ordre : <b>{driver.get('order','')}</b></div><div>Statut : <b>{'EN RELAIS' if is_current else 'Disponible'}</b></div></div>""",unsafe_allow_html=True)
    st.subheader("Statistiques pilotes"); summary=build_driver_summary(); st.dataframe(summary,use_container_width=True) if not summary.empty else st.info("Aucun tour pilote enregistré.")
    if PLOTLY_OK:
        laps=load_lap_events(MY_BIKE)
        if not laps.empty:
            fig=px.line(laps,x="tour_total",y="lap_seconds",color="pilote",markers=True,title="Temps au tour par pilote"); fig.update_yaxes(title="Temps au tour (s)",autorange="reversed"); st.plotly_chart(fig,use_container_width=True)

elif page=="Concurrents":
    st.header("Concurrents - Relais et arrêts"); cat=st.selectbox("Catégorie à surveiller",["PRD","EWC","SST","EXP","ALL"],key="competitor_category"); st.subheader("Classement live filtré"); comp=df_live.copy(); comp=comp if cat=="ALL" else comp[comp["Cat"]==cat]; cols=["Pos.","Cat.P","No.","Cat","Team","Rider","Laps","L. Lap","Best Lap","Last Pit","Last Pit Time","Total Pit","Total pit time"]; st.dataframe(comp[[c for c in cols if c in comp.columns]],use_container_width=True,height=350); st.subheader("Nombre de tours par relais"); m=build_relay_matrix(cat); st.dataframe(m,use_container_width=True) if not m.empty else st.info("Aucun relais détecté."); st.subheader("Temps de chaque arrêt - Last Pit Time"); p=build_pit_time_matrix(cat); st.dataframe(p,use_container_width=True) if not p.empty else st.info("Aucun temps d'arrêt enregistré.")

elif page=="Détail concurrent":
    st.header("Détail concurrent"); opts=df_live[df_live["No."].astype(str)!=MY_BIKE]["No."].astype(str).tolist() if not df_live.empty else []
    if opts:
        selected=st.selectbox("Concurrent",opts); row=get_current_bike_row(df_live,selected); st.dataframe(pd.DataFrame([row]),use_container_width=True) if row is not None else None; rel=load_relay_events(); st.subheader("Relais"); st.dataframe(rel[rel["numero"].astype(str)==str(selected)] if not rel.empty else pd.DataFrame(),use_container_width=True); st.subheader("Stats tours"); stats=lap_stats_for_bike(selected); st.json({k:seconds_to_laptime(v) if k in ["best","avg","median","pace5","recent10"] else v for k,v in stats.items()}); laps=load_lap_events(selected)
        if PLOTLY_OK and not laps.empty:
            fig=px.line(laps,x="tour_total",y="lap_seconds",color="pilote",markers=True,title=f"Temps au tour #{selected}"); fig.update_yaxes(autorange="reversed"); st.plotly_chart(fig,use_container_width=True)
    else: st.info("Aucun concurrent disponible.")

elif page=="Comparatif 96 vs concurrent":
    st.header(f"Comparatif {MY_BIKE} vs concurrent"); opts=df_live[df_live["No."].astype(str)!=MY_BIKE]["No."].astype(str).tolist() if not df_live.empty else []
    if opts:
        selected=st.selectbox("Concurrent à comparer",opts); comp=build_comparison(MY_BIKE,selected)
        if comp.empty: st.info("Pas encore assez de relais enregistrés pour faire le comparatif.")
        else:
            st.dataframe(comp,use_container_width=True)
            if PLOTLY_OK:
                fig=go.Figure(); fig.add_bar(x=comp["Relais"],y=comp["Gain/perte piste (s)"],name="Piste"); fig.add_bar(x=comp["Relais"],y=comp["Gain/perte stand (s)"],name="Stand"); fig.add_scatter(x=comp["Relais"],y=comp["Cumul (s)"],name="Cumul",mode="lines+markers"); fig.update_layout(title="Gain/perte par relais",barmode="relative",xaxis_title="Relais",yaxis_title="Secondes"); st.plotly_chart(fig,use_container_width=True)
    else: st.info("Aucun concurrent disponible.")

elif page=="Stratégie":
    st.header("Stratégie"); metrics=calculate_bike_metrics(df_live)
    if metrics is None: st.warning("Pas assez de données.")
    else:
        b=metrics["row"]; current_lap=to_int(b.get("Laps")) or 0; c=st.columns(3); c[0].metric("Carburant restant estimé",f"{metrics['fuel_remaining']:.1f} L"); c[1].metric("Tours restants",metrics["laps_remaining"]); c[2].metric("Pit window",metrics["pit_window"]); st.subheader("Programmer un événement stratégique"); a,bcol,ccol=st.columns([2,2,1])
        with a: event_lap=st.number_input("Tour",min_value=current_lap,max_value=current_lap+150,value=current_lap+max(metrics["laps_remaining"],1),step=1)
        with bcol: event_type=st.selectbox("Type d'événement",["PIT","Changement pilote","Changement pneus","Safety Car","FCY","Freins","Note"])
        with ccol:
            st.write(""); st.write("")
            if st.button("Ajouter"):
                st.session_state.planned_events.append({"lap":int(event_lap),"type":event_type,"created_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}); st.rerun()
        if st.button("Effacer les événements planifiés"): st.session_state.planned_events=[]; st.rerun()
        timeline=build_strategy_timeline(current_lap,50); st.subheader("Timeline stratégique"); st.dataframe(timeline.style.apply(color_timeline,axis=1),use_container_width=True,height=500); st.subheader("Événements planifiés"); st.dataframe(pd.DataFrame(st.session_state.planned_events),use_container_width=True)

elif page=="Simulation":
    st.header("Mode simulation"); st.write("Permet de tester les relais, pit stops, comparatif concurrent, statistiques pilotes et exports sans course live."); c=st.columns(3)
    with c[0]:
        if st.button("Réinitialiser simulation"): reset_simulation(False); st.success("Simulation réinitialisée."); st.rerun()
    with c[1]:
        if st.button("Réinitialiser simulation + historique"): reset_simulation(True); st.success("Simulation et historique réinitialisés."); st.rerun()
    with c[2]: st.metric("Source actuelle",DATA_SOURCE)
    if st.session_state.sim_state: st.json({"started_at":st.session_state.sim_state.get("started_at"),"tick":st.session_state.sim_state.get("tick"),"bikes":len(st.session_state.sim_state.get("bikes",{}))})
    st.subheader("Données simulées"); st.dataframe(df_live,use_container_width=True,height=400)

elif page=="Historique & exports":
    st.header("Historique & exports"); rel=load_relay_events(); laps=load_lap_events(); snaps=load_snapshots(); c=st.columns(3); c[0].metric("Relais enregistrés",len(rel)); c[1].metric("Tours enregistrés",len(laps)); c[2].metric("Snapshots",len(snaps)); st.download_button("Télécharger relais CSV",rel.to_csv(index=False).encode("utf-8-sig"),"relay_events.csv","text/csv"); st.download_button("Télécharger tours CSV",laps.to_csv(index=False).encode("utf-8-sig"),"lap_events.csv","text/csv"); st.download_button("Télécharger Excel complet",export_excel_bytes(),"race_engineer_export.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    with st.expander("Historique relais"): st.dataframe(rel,use_container_width=True)
    with st.expander("Historique tours"): st.dataframe(laps,use_container_width=True)
    with st.expander("Snapshots"): st.dataframe(snaps[["id","fetched_at","source"]] if not snaps.empty else snaps,use_container_width=True)

elif page=="Paramètres":
    st.header("Paramètres")
    with st.form("settings_form"):
        st.subheader("Source de données"); data_source=st.selectbox("Source",["Live","Simulation"],index=0 if CONFIG.get("data_source")=="Live" else 1); live_url=st.text_input("URL live timing JSON",value=CONFIG.get("live_url",DEFAULT_LIVE_URL)); simulation_speed=st.number_input("Vitesse simulation",min_value=1,max_value=10,value=int(CONFIG.get("simulation_speed",1)),step=1)
        st.subheader("Paramètres équipe"); my_bike=st.text_input("Numéro de notre moto",value=str(CONFIG.get("my_bike","96"))); team_name=st.text_input("Nom de l'équipe",value=CONFIG.get("team_name","LEGACY COMPETITION")); category=st.selectbox("Catégorie",["PRD","EWC","SST","EXP","ALL"],index=["PRD","EWC","SST","EXP","ALL"].index(CONFIG.get("category","PRD"))); refresh_ms=st.number_input("Rafraîchissement live (millisecondes)",min_value=1000,max_value=30000,value=int(CONFIG.get("refresh_ms",1000)),step=1000)
        st.subheader("Carburant / pneus / freins"); fuel_capacity_l=st.number_input("Capacité réservoir estimée (L)",value=float(CONFIG.get("fuel_capacity_l",24.0)),step=0.5); fuel_per_lap_l=st.number_input("Consommation estimée (L/tour)",value=float(CONFIG.get("fuel_per_lap_l",0.70)),step=0.01); fuel_safety_laps=st.number_input("Marge sécurité carburant (tours)",value=int(CONFIG.get("fuel_safety_laps",2)),step=1); front_tire_life_laps=st.number_input("Durée estimée pneu avant (tours)",value=int(CONFIG.get("front_tire_life_laps",80)),step=1); rear_tire_life_laps=st.number_input("Durée estimée pneu arrière (tours)",value=int(CONFIG.get("rear_tire_life_laps",55)),step=1); brake_life_laps=st.number_input("Durée estimée freins (tours)",value=int(CONFIG.get("brake_life_laps",220)),step=1)
        st.subheader("Pilotes / brassards"); current_drivers=CONFIG.get("drivers",DEFAULT_CONFIG["drivers"]); driver_count=st.selectbox("Nombre de pilotes",[3,4],index=0 if len(current_drivers)<=3 else 1)
        while len(current_drivers)<driver_count: current_drivers.append({"name":f"Pilote {len(current_drivers)+1}","armband":"","color":"#ffffff","order":len(current_drivers)+1,"active":True})
        new_drivers=[]
        for i in range(driver_count):
            d=current_drivers[i]; st.markdown(f"### Pilote {i+1}"); a,b,c,dcol=st.columns([3,2,2,1])
            with a: name=st.text_input(f"Nom pilote {i+1}",value=d.get("name",""),key=f"driver_name_{i}")
            with b: armband=st.text_input(f"Brassard {i+1}",value=d.get("armband",""),key=f"driver_armband_{i}")
            with c: color=st.color_picker(f"Couleur {i+1}",value=d.get("color","#ffffff"),key=f"driver_color_{i}")
            with dcol: order=st.number_input(f"Ordre {i+1}",min_value=1,max_value=4,value=int(d.get("order",i+1)),step=1,key=f"driver_order_{i}")
            active=st.checkbox(f"Pilote {i+1} actif",value=bool(d.get("active",True)),key=f"driver_active_{i}"); new_drivers.append({"name":name,"armband":armband,"color":color,"order":int(order),"active":active})
        submitted=st.form_submit_button("Enregistrer les paramètres")
    if submitted:
        CONFIG.update({"data_source":data_source,"live_url":live_url,"simulation_speed":int(simulation_speed),"my_bike":str(my_bike),"team_name":team_name,"category":category,"refresh_ms":int(refresh_ms),"fuel_capacity_l":float(fuel_capacity_l),"fuel_per_lap_l":float(fuel_per_lap_l),"fuel_safety_laps":int(fuel_safety_laps),"front_tire_life_laps":int(front_tire_life_laps),"rear_tire_life_laps":int(rear_tire_life_laps),"brake_life_laps":int(brake_life_laps),"drivers":sorted(new_drivers,key=lambda x:x["order"])}); st.session_state.config=CONFIG; save_config(CONFIG); st.success("Paramètres enregistrés."); st.rerun()
