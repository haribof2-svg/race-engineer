# Race Engineer - Endurance Moto
# Application Streamlit complete pour suivi live, relais, pilotes, concurrents,
# comparatif strategic et premiere prediction.

import json
import re
import sqlite3
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

try:
    import plotly.express as px
except Exception:
    px = None

# ============================================================
# CONFIGURATION GENERALE
# ============================================================

st.set_page_config(page_title="Race Engineer - Endurance Moto", layout="wide")

DEFAULT_LIVE_URL = "https://fimewc.live-frclassification.fr/r1.json"
DB_PATH = Path("race_engineer_history.sqlite")
CONFIG_PATH = Path("race_engineer_config.json")

DEFAULT_CONFIG: Dict[str, Any] = {
    "live_url": DEFAULT_LIVE_URL,
    "my_bike": "96",
    "team_name": "LEGACY COMPETITION",
    "category": "PRD",
    "refresh_ms": 1000,
    "save_every_snapshot": True,
    "fuel_capacity_l": 24.0,
    "fuel_per_lap_l": 0.70,
    "fuel_safety_laps": 2,
    "fuel_fill_l": 24.0,
    "tyre_tracking_enabled": True,
    "tyre_life_laps": 45,
    "tyre_safety_laps": 5,
    "brake_tracking_enabled": False,
    "brake_life_laps": 600,
    "drivers": [
        {"name": "Pilote 1", "armband": "Rouge", "color": "#ff4b4b", "order": 1, "active": True},
        {"name": "Pilote 2", "armband": "Bleu", "color": "#4b8bff", "order": 2, "active": True},
        {"name": "Pilote 3", "armband": "Jaune", "color": "#ffd84b", "order": 3, "active": True},
    ],
}

CATEGORIES = ["PRD", "EWC", "SST", "EXP", "ALL"]
EVENT_TYPES = ["PIT", "Changement pilote", "Changement pneus", "Safety Car", "Full Course Yellow", "Freins", "Note"]

# ============================================================
# ETAT STREAMLIT
# ============================================================

if "bike_state" not in st.session_state:
    st.session_state.bike_state = {}
if "relay_history" not in st.session_state:
    st.session_state.relay_history = []
if "pilot_lap_history" not in st.session_state:
    st.session_state.pilot_lap_history = []
if "config" not in st.session_state:
    st.session_state.config = None

# ============================================================
# OUTILS GENERAUX
# ============================================================

def deep_merge_config(default: Dict[str, Any], user: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(default)
    for key, value in user.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge_config(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            with CONFIG_PATH.open("r", encoding="utf-8") as f:
                user_config = json.load(f)
            return deep_merge_config(DEFAULT_CONFIG, user_config)
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config(config: Dict[str, Any]) -> None:
    try:
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        st.warning(f"Impossible de sauvegarder les parametres : {exc}")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    value = str(value)
    value = re.sub(r"\{.*?\}", "", value)
    return value.strip()


def to_int(value: Any) -> Optional[int]:
    try:
        value = clean_text(value)
        value = value.replace("Lp.", "").replace("Lp", "").strip()
        if value in ["", "-"]:
            return None
        return int(float(value))
    except Exception:
        return None


def parse_time_to_seconds(value: Any) -> Optional[float]:
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


def seconds_to_laptime(seconds: Optional[float]) -> str:
    if seconds is None or pd.isna(seconds):
        return "-"
    seconds = float(seconds)
    if seconds < 0:
        return "-" + seconds_to_laptime(abs(seconds))
    hours = int(seconds // 3600)
    remaining = seconds - hours * 3600
    minutes = int(remaining // 60)
    rest = remaining - minutes * 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{rest:06.3f}"
    return f"{minutes}:{rest:06.3f}"


def signed_seconds(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "-"
    sign = "+" if value >= 0 else "-"
    return f"{sign}{seconds_to_laptime(abs(value))}"


def format_duration_ms(ms: Any) -> str:
    try:
        total_seconds = int(float(ms) / 1000)
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        return "-"


def is_pit_in(value: Any) -> bool:
    return "Pit In" in clean_text(value)


def is_pit_out(value: Any) -> bool:
    return "Pit Out" in clean_text(value)


def safe_div(a: float, b: float) -> Optional[float]:
    try:
        if b == 0:
            return None
        return a / b
    except Exception:
        return None


def style_gain_loss(value: Any) -> str:
    if value is None or value == "-":
        return ""
    try:
        numeric = float(value)
    except Exception:
        txt = str(value)
        if txt.startswith("+"):
            return "background-color:#0b3d1d;color:white"
        if txt.startswith("-"):
            return "background-color:#4d1111;color:white"
        return ""
    if numeric > 0:
        return "background-color:#0b3d1d;color:white"
    if numeric < 0:
        return "background-color:#4d1111;color:white"
    return ""


# ============================================================
# CONFIG INIT + REFRESH
# ============================================================

if st.session_state.config is None:
    st.session_state.config = load_config()

CONFIG: Dict[str, Any] = st.session_state.config
LIVE_URL = CONFIG.get("live_url", DEFAULT_LIVE_URL)
MY_BIKE = str(CONFIG.get("my_bike", "96"))
REFRESH_MS = int(CONFIG.get("refresh_ms", 1000))
st_autorefresh(interval=REFRESH_MS, key="live_refresh")

# ============================================================
# BASE DE DONNEES + MIGRATIONS
# ============================================================

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
            event_time_utc TEXT,
            payload TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
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
            last_pit_seconds REAL,
            tour_total INTEGER,
            total_pit INTEGER
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lap_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            numero TEXT,
            categorie TEXT,
            team TEXT,
            pilote TEXT,
            tour_total INTEGER,
            relay_no INTEGER,
            last_pit INTEGER,
            total_pit INTEGER,
            lap_time TEXT,
            lap_seconds REAL,
            position TEXT,
            position_categorie TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pilot_laps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            numero TEXT,
            pilote TEXT,
            tour_total INTEGER,
            lap_time TEXT,
            lap_seconds REAL,
            relay_laps INTEGER
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            lap INTEGER,
            type TEXT,
            driver TEXT,
            note TEXT
        )
        """
    )

    conn.commit()
    migrate_schema(conn)
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cols = table_columns(conn, table)
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.commit()


def migrate_schema(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "snapshots", "event_time_utc", "TEXT")
    ensure_column(conn, "relay_events", "last_pit_seconds", "REAL")
    # pilot_laps is old compatibility table; lap_events is the new complete table.


def run_query_df(query: str, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
    conn = db_connect()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()
    return df


def get_scalar(query: str, params: Tuple[Any, ...] = ()) -> Any:
    conn = db_connect()
    try:
        row = conn.execute(query, params).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return row[0]


def save_snapshot_to_db(data: Dict[str, Any]) -> None:
    if not CONFIG.get("save_every_snapshot", True):
        return
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO snapshots (fetched_at, event_time_utc, payload) VALUES (?, ?, ?)",
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                data.get("HeureJourUTC"),
                json.dumps(data, ensure_ascii=False),
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def relay_already_saved(numero: str, total_pit: Optional[int]) -> bool:
    if total_pit is None:
        return False
    found = get_scalar("SELECT id FROM relay_events WHERE numero = ? AND total_pit = ? LIMIT 1", (numero, total_pit))
    return found is not None


def save_relay_event_to_db(event: Dict[str, Any]) -> None:
    try:
        if relay_already_saved(event["numero"], event.get("total_pit")):
            return
        conn = db_connect()
        conn.execute(
            """
            INSERT INTO relay_events (
                timestamp, numero, categorie, team, pilote, relais,
                tours_relais, last_pit_time, last_pit_seconds, tour_total, total_pit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["timestamp"],
                event["numero"],
                event["categorie"],
                event["team"],
                event["pilote"],
                event["relais"],
                event["tours_relais"],
                event["last_pit_time"],
                event.get("last_pit_seconds"),
                event["tour_total"],
                event["total_pit"],
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        st.warning(f"Erreur sauvegarde relais : {exc}")


def lap_already_saved(numero: str, tour_total: Optional[int]) -> bool:
    if tour_total is None:
        return True
    found = get_scalar("SELECT id FROM lap_events WHERE numero = ? AND tour_total = ? LIMIT 1", (numero, tour_total))
    return found is not None


def save_lap_event_to_db(event: Dict[str, Any]) -> None:
    try:
        if lap_already_saved(event["numero"], event.get("tour_total")):
            return
        conn = db_connect()
        conn.execute(
            """
            INSERT INTO lap_events (
                timestamp, numero, categorie, team, pilote, tour_total,
                relay_no, last_pit, total_pit, lap_time, lap_seconds,
                position, position_categorie
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["timestamp"], event["numero"], event["categorie"], event["team"],
                event["pilote"], event["tour_total"], event["relay_no"], event["last_pit"],
                event["total_pit"], event["lap_time"], event["lap_seconds"],
                event["position"], event["position_categorie"],
            ),
        )
        # Old compatibility for the page built earlier.
        if event["numero"] == MY_BIKE:
            conn.execute(
                """
                INSERT INTO pilot_laps (timestamp, numero, pilote, tour_total, lap_time, lap_seconds, relay_laps)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["timestamp"], event["numero"], event["pilote"], event["tour_total"],
                    event["lap_time"], event["lap_seconds"], event["last_pit"],
                ),
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


def save_strategy_event(lap: int, event_type: str, driver: str, note: str) -> None:
    conn = db_connect()
    conn.execute(
        "INSERT INTO strategy_events (created_at, lap, type, driver, note) VALUES (?, ?, ?, ?, ?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), lap, event_type, driver, note),
    )
    conn.commit()
    conn.close()


def clear_strategy_events() -> None:
    conn = db_connect()
    conn.execute("DELETE FROM strategy_events")
    conn.commit()
    conn.close()


def load_strategy_events() -> pd.DataFrame:
    return run_query_df("SELECT id, created_at, lap, type, driver, note FROM strategy_events ORDER BY lap ASC, id ASC")


def load_relay_events_from_db(numero: Optional[str] = None) -> pd.DataFrame:
    if numero:
        return run_query_df(
            """
            SELECT timestamp, numero, categorie, team, pilote, relais, tours_relais,
                   last_pit_time, last_pit_seconds, tour_total, total_pit
            FROM relay_events WHERE numero = ? ORDER BY id ASC
            """,
            (str(numero),),
        )
    return run_query_df(
        """
        SELECT timestamp, numero, categorie, team, pilote, relais, tours_relais,
               last_pit_time, last_pit_seconds, tour_total, total_pit
        FROM relay_events ORDER BY id ASC
        """
    )


def load_lap_events_from_db(numero: Optional[str] = None) -> pd.DataFrame:
    if numero:
        return run_query_df(
            """
            SELECT timestamp, numero, categorie, team, pilote, tour_total, relay_no,
                   last_pit, total_pit, lap_time, lap_seconds, position, position_categorie
            FROM lap_events WHERE numero = ? ORDER BY tour_total ASC
            """,
            (str(numero),),
        )
    return run_query_df(
        """
        SELECT timestamp, numero, categorie, team, pilote, tour_total, relay_no,
               last_pit, total_pit, lap_time, lap_seconds, position, position_categorie
        FROM lap_events ORDER BY numero ASC, tour_total ASC
        """
    )


def load_snapshots_list(limit: int = 500) -> pd.DataFrame:
    return run_query_df(
        "SELECT id, fetched_at, event_time_utc FROM snapshots ORDER BY id DESC LIMIT ?",
        (limit,),
    )


def load_snapshot_payload(snapshot_id: int) -> Optional[Dict[str, Any]]:
    payload = get_scalar("SELECT payload FROM snapshots WHERE id = ?", (snapshot_id,))
    if not payload:
        return None
    try:
        return json.loads(payload)
    except Exception:
        return None


def get_last_recorded_lap_from_db(numero: str) -> Optional[int]:
    return get_scalar("SELECT MAX(tour_total) FROM lap_events WHERE numero = ?", (str(numero),))


def get_last_recorded_total_pit_from_db(numero: str) -> Optional[int]:
    return get_scalar("SELECT MAX(total_pit) FROM relay_events WHERE numero = ?", (str(numero),))

# ============================================================
# LIVE DATA
# ============================================================

def fetch_live_data() -> Optional[Dict[str, Any]]:
    try:
        ts = int(datetime.now().timestamp() * 1000)
        separator = "&" if "?" in LIVE_URL else "?"
        url = f"{LIVE_URL}{separator}t={ts}"
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,text/plain,*/*",
                "Cache-Control": "no-cache",
            },
            timeout=10,
        )
        response.raise_for_status()
        text = response.content.decode("utf-8-sig")
        return json.loads(text)
    except Exception as exc:
        st.error(f"Impossible de recuperer les donnees live : {exc}")
        return None


def payload_to_dataframe(data: Dict[str, Any]) -> pd.DataFrame:
    columns = [c.get("Texte") or c.get("Nom") or f"Col{i}" for i, c in enumerate(data.get("Colonnes", []))]
    df = pd.DataFrame(data.get("Donnees", []), columns=columns)
    if "" in df.columns:
        df = df.rename(columns={"": "Etat"})
    return df


def process_live_state(df: pd.DataFrame) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    required = ["No.", "Cat", "Team", "Rider", "Laps", "Last Pit", "Last Pit Time", "Total Pit", "L. Lap"]
    if any(col not in df.columns for col in required):
        return

    for _, row in df.iterrows():
        number = clean_text(row.get("No."))
        if not number:
            continue

        category = clean_text(row.get("Cat"))
        team = clean_text(row.get("Team"))
        rider = clean_text(row.get("Rider")).replace("*", "").strip()
        laps = to_int(row.get("Laps"))
        last_pit = to_int(row.get("Last Pit"))
        last_pit_time = clean_text(row.get("Last Pit Time"))
        last_pit_seconds = parse_time_to_seconds(last_pit_time)
        total_pit = to_int(row.get("Total Pit"))
        last_lap = clean_text(row.get("L. Lap"))
        last_lap_seconds = parse_time_to_seconds(last_lap)
        position = clean_text(row.get("Pos."))
        position_cat = clean_text(row.get("Cat.P"))
        relay_no_current = (total_pit or 0) + 1

        if number not in st.session_state.bike_state:
            last_db_lap = get_last_recorded_lap_from_db(number)
            last_db_total_pit = get_last_recorded_total_pit_from_db(number)
            st.session_state.bike_state[number] = {
                "previous_last_pit": last_pit,
                "previous_total_pit": total_pit,
                "last_recorded_total_pit": last_db_total_pit if last_db_total_pit is not None else total_pit,
                "last_recorded_lap": last_db_lap if last_db_lap is not None else laps,
            }
            continue

        state = st.session_state.bike_state[number]
        previous_last_pit = state.get("previous_last_pit")
        previous_total_pit = state.get("previous_total_pit")
        last_recorded_total_pit = state.get("last_recorded_total_pit")
        last_recorded_lap = state.get("last_recorded_lap")

        # 1) Enregistrement d'un tour termine.
        if laps is not None and last_lap_seconds is not None:
            if last_recorded_lap is not None and laps > last_recorded_lap:
                lap_event = {
                    "timestamp": now,
                    "numero": number,
                    "categorie": category,
                    "team": team,
                    "pilote": rider,
                    "tour_total": laps,
                    "relay_no": relay_no_current,
                    "last_pit": last_pit,
                    "total_pit": total_pit,
                    "lap_time": last_lap,
                    "lap_seconds": last_lap_seconds,
                    "position": position,
                    "position_categorie": position_cat,
                }
                save_lap_event_to_db(lap_event)
                state["last_recorded_lap"] = laps

        # 2) Detection d'un arret / fin de relais.
        stop_validated = False
        if total_pit is not None and previous_total_pit is not None:
            if total_pit > previous_total_pit and total_pit != last_recorded_total_pit:
                stop_validated = True
        if (
            last_pit == 0
            and previous_last_pit is not None
            and previous_last_pit > 0
            and (is_pit_in(last_lap) or is_pit_out(last_lap))
            and total_pit != last_recorded_total_pit
        ):
            stop_validated = True

        if stop_validated:
            relay_laps = previous_last_pit
            relay_no_finished = total_pit if total_pit is not None else None
            if relay_laps is not None and relay_laps > 0:
                event = {
                    "timestamp": now,
                    "numero": number,
                    "categorie": category,
                    "team": team,
                    "pilote": rider,
                    "relais": relay_no_finished,
                    "tours_relais": relay_laps,
                    "last_pit_time": last_pit_time,
                    "last_pit_seconds": last_pit_seconds,
                    "tour_total": laps,
                    "total_pit": total_pit,
                }
                st.session_state.relay_history.append(event)
                save_relay_event_to_db(event)
                state["last_recorded_total_pit"] = total_pit

        state["previous_last_pit"] = last_pit
        state["previous_total_pit"] = total_pit

# ============================================================
# CALCULS METIERS
# ============================================================

def get_current_bike_row(df_live: pd.DataFrame, numero: Optional[str] = None) -> Optional[pd.Series]:
    numero = str(numero or MY_BIKE)
    if df_live.empty or "No." not in df_live.columns:
        return None
    bike = df_live[df_live["No."].astype(str) == numero]
    if bike.empty:
        return None
    return bike.iloc[0]


def calculate_bike_metrics(df_live: pd.DataFrame, numero: Optional[str] = None) -> Optional[Dict[str, Any]]:
    row = get_current_bike_row(df_live, numero)
    if row is None:
        return None
    last_pit = to_int(row.get("Last Pit")) or 0
    fuel_capacity = float(CONFIG.get("fuel_capacity_l", 24.0))
    fuel_per_lap = float(CONFIG.get("fuel_per_lap_l", 0.70))
    safety_laps = int(CONFIG.get("fuel_safety_laps", 2))
    tyre_life = int(CONFIG.get("tyre_life_laps", 45))
    tyre_safety = int(CONFIG.get("tyre_safety_laps", 5))
    brake_life = int(CONFIG.get("brake_life_laps", 600))

    fuel_used = last_pit * fuel_per_lap
    fuel_remaining = max(fuel_capacity - fuel_used, 0)
    laps_remaining = int(fuel_remaining / fuel_per_lap) if fuel_per_lap > 0 else 0
    pit_window_min = max(laps_remaining - safety_laps, 0)

    tyre_remaining = max(tyre_life - last_pit, 0)
    tyre_pct = max(0.0, min(1.0, tyre_remaining / tyre_life)) if tyre_life > 0 else 0
    tyre_window_min = max(tyre_remaining - tyre_safety, 0)

    total_laps = to_int(row.get("Laps")) or 0
    brake_remaining = max(brake_life - total_laps, 0)
    brake_pct = max(0.0, min(1.0, brake_remaining / brake_life)) if brake_life > 0 else 0

    return {
        "row": row,
        "last_pit": last_pit,
        "fuel_remaining": fuel_remaining,
        "laps_remaining": laps_remaining,
        "pit_window": f"{pit_window_min}-{laps_remaining}",
        "tyre_remaining": tyre_remaining,
        "tyre_pct": tyre_pct,
        "tyre_window": f"{tyre_window_min}-{tyre_remaining}",
        "brake_remaining": brake_remaining,
        "brake_pct": brake_pct,
    }


def build_relay_matrix(category_filter: str = "PRD") -> pd.DataFrame:
    df_hist = load_relay_events_from_db()
    if df_hist.empty:
        return pd.DataFrame()
    if category_filter != "ALL":
        df_hist = df_hist[df_hist["categorie"] == category_filter]
    if df_hist.empty:
        return pd.DataFrame()
    matrix = df_hist.pivot_table(
        index=["numero", "team"], columns="relais", values="tours_relais", aggfunc="last"
    ).reset_index()
    matrix.columns = [f"Relais N°{c}" if isinstance(c, int) or str(c).isdigit() else c for c in matrix.columns]
    return matrix


def build_pit_time_matrix(category_filter: str = "PRD") -> pd.DataFrame:
    df_hist = load_relay_events_from_db()
    if df_hist.empty:
        return pd.DataFrame()
    if category_filter != "ALL":
        df_hist = df_hist[df_hist["categorie"] == category_filter]
    if df_hist.empty:
        return pd.DataFrame()
    matrix = df_hist.pivot_table(
        index=["numero", "team"], columns="relais", values="last_pit_time", aggfunc="last"
    ).reset_index()
    matrix.columns = [f"Arrêt N°{c}" if isinstance(c, int) or str(c).isdigit() else c for c in matrix.columns]
    return matrix


def bike_lap_summary(numero: str) -> Dict[str, Any]:
    laps = load_lap_events_from_db(numero)
    relays = load_relay_events_from_db(numero)
    if laps.empty:
        return {
            "laps_df": laps,
            "relays_df": relays,
            "summary": {},
            "relay_summary": pd.DataFrame(),
        }

    valid = laps.dropna(subset=["lap_seconds"]).copy()
    summary = {
        "tours_enregistres": len(valid),
        "meilleur_tour": valid["lap_seconds"].min() if not valid.empty else None,
        "moyenne": valid["lap_seconds"].mean() if not valid.empty else None,
        "mediane": valid["lap_seconds"].median() if not valid.empty else None,
        "regularite_std": valid["lap_seconds"].std() if len(valid) > 1 else None,
        "pace_5_best": valid.nsmallest(min(5, len(valid)), "lap_seconds")["lap_seconds"].mean() if not valid.empty else None,
        "moyenne_10_derniers": valid.tail(10)["lap_seconds"].mean() if not valid.empty else None,
        "nb_relais_laps": valid["relay_no"].nunique() if "relay_no" in valid.columns else None,
    }

    relay_rows = []
    for relay_no, group in valid.groupby("relay_no"):
        relay_rows.append({
            "Relais": int(relay_no) if pd.notna(relay_no) else None,
            "Tours enregistrés": len(group),
            "Tour début": int(group["tour_total"].min()) if pd.notna(group["tour_total"].min()) else None,
            "Tour fin": int(group["tour_total"].max()) if pd.notna(group["tour_total"].max()) else None,
            "Pilote(s)": ", ".join(sorted(set(group["pilote"].dropna().astype(str)))),
            "Moyenne relais": seconds_to_laptime(group["lap_seconds"].mean()),
            "Meilleur relais": seconds_to_laptime(group["lap_seconds"].min()),
            "Médiane relais": seconds_to_laptime(group["lap_seconds"].median()),
            "Régularité std": round(group["lap_seconds"].std(), 3) if len(group) > 1 else None,
            "Temps piste total sec": round(group["lap_seconds"].sum(), 3),
            "Temps piste total": seconds_to_laptime(group["lap_seconds"].sum()),
        })
    relay_summary = pd.DataFrame(relay_rows).sort_values("Relais") if relay_rows else pd.DataFrame()

    return {
        "laps_df": laps,
        "relays_df": relays,
        "summary": summary,
        "relay_summary": relay_summary,
    }


def driver_stats_for_name(name: str, laps_df: pd.DataFrame) -> Dict[str, Any]:
    if laps_df.empty:
        return {}
    name_norm = name.strip().lower()
    if not name_norm:
        return {}
    mask = laps_df["pilote"].fillna("").str.lower().apply(lambda x: name_norm in x or x in name_norm)
    group = laps_df[mask].dropna(subset=["lap_seconds"]).copy()
    if group.empty:
        return {}

    relay_sizes = group.groupby("relay_no").size()
    relay_avg = group.groupby("relay_no")["lap_seconds"].mean()
    last_relay_no = group["relay_no"].max()
    last_relay_laps = int(relay_sizes.loc[last_relay_no]) if last_relay_no in relay_sizes.index else None
    current_relay_no = group["relay_no"].iloc[-1]
    current_relay = group[group["relay_no"] == current_relay_no]

    return {
        "Tours total": len(group),
        "Relais total": int(group["relay_no"].nunique()),
        "Tours dernier relais": last_relay_laps,
        "Tours moyens / relais": round(relay_sizes.mean(), 1) if not relay_sizes.empty else None,
        "Tours max relais": int(relay_sizes.max()) if not relay_sizes.empty else None,
        "Tours min relais": int(relay_sizes.min()) if not relay_sizes.empty else None,
        "Meilleur tour": seconds_to_laptime(group["lap_seconds"].min()),
        "Chrono moyen total": seconds_to_laptime(group["lap_seconds"].mean()),
        "Chrono médian": seconds_to_laptime(group["lap_seconds"].median()),
        "Chrono moyen dernier relais": seconds_to_laptime(relay_avg.loc[last_relay_no]) if last_relay_no in relay_avg.index else "-",
        "Chrono moyen relais en cours": seconds_to_laptime(current_relay["lap_seconds"].mean()),
        "Régularité std": round(group["lap_seconds"].std(), 3) if len(group) > 1 else None,
        "Pace 5 meilleurs": seconds_to_laptime(group.nsmallest(min(5, len(group)), "lap_seconds")["lap_seconds"].mean()),
        "Moyenne 10 derniers": seconds_to_laptime(group.tail(10)["lap_seconds"].mean()),
    }


def build_strategy_timeline(current_lap: int, future_laps: int = 50) -> pd.DataFrame:
    planned = load_strategy_events()
    start_lap = max(current_lap - 10, 0)
    end_lap = current_lap + future_laps
    rows = []
    for lap in range(start_lap, end_lap + 1):
        events = []
        if not planned.empty:
            events = planned[planned["lap"] == lap]["type"].astype(str).tolist()
        rows.append({
            "Tour": lap,
            "Zone": "Passé" if lap <= current_lap else "Futur",
            "Événement planifié": " / ".join(events),
        })
    return pd.DataFrame(rows)


def color_timeline(row: pd.Series) -> List[str]:
    event = str(row.get("Événement planifié", ""))
    zone = str(row.get("Zone", ""))
    if event:
        return ["background-color:#5c3b00;color:white"] * len(row)
    if zone == "Futur":
        return ["background-color:#102033;color:white"] * len(row)
    return [""] * len(row)


def build_comparison(my_numero: str, competitor_numero: str) -> pd.DataFrame:
    my_laps = load_lap_events_from_db(my_numero).dropna(subset=["lap_seconds"])
    cp_laps = load_lap_events_from_db(competitor_numero).dropna(subset=["lap_seconds"])
    my_relays = load_relay_events_from_db(my_numero)
    cp_relays = load_relay_events_from_db(competitor_numero)

    relay_ids = set()
    if not my_laps.empty:
        relay_ids.update(my_laps["relay_no"].dropna().astype(int).tolist())
    if not cp_laps.empty:
        relay_ids.update(cp_laps["relay_no"].dropna().astype(int).tolist())
    if not my_relays.empty:
        relay_ids.update(my_relays["relais"].dropna().astype(int).tolist())
    if not cp_relays.empty:
        relay_ids.update(cp_relays["relais"].dropna().astype(int).tolist())

    rows = []
    cumulative = 0.0
    for relay in sorted(relay_ids):
        ml = my_laps[my_laps["relay_no"] == relay]
        cl = cp_laps[cp_laps["relay_no"] == relay]
        mr = my_relays[my_relays["relais"] == relay]
        cr = cp_relays[cp_relays["relais"] == relay]

        my_laps_count = len(ml)
        cp_laps_count = len(cl)
        my_avg = ml["lap_seconds"].mean() if my_laps_count else None
        cp_avg = cl["lap_seconds"].mean() if cp_laps_count else None
        my_total_track = ml["lap_seconds"].sum() if my_laps_count else None
        cp_total_track = cl["lap_seconds"].sum() if cp_laps_count else None
        comparable_laps = min(my_laps_count, cp_laps_count)

        gain_track_norm = None
        if my_avg is not None and cp_avg is not None and comparable_laps > 0:
            # Positif = notre moto gagne du temps en piste.
            gain_track_norm = (cp_avg - my_avg) * comparable_laps

        raw_track_delta = None
        if my_total_track is not None and cp_total_track is not None:
            raw_track_delta = cp_total_track - my_total_track

        my_pit = None
        cp_pit = None
        if not mr.empty:
            my_pit = mr.iloc[-1].get("last_pit_seconds")
        if not cr.empty:
            cp_pit = cr.iloc[-1].get("last_pit_seconds")

        gain_stand = None
        if my_pit is not None and cp_pit is not None and pd.notna(my_pit) and pd.notna(cp_pit):
            # Positif = notre arret est plus rapide.
            gain_stand = float(cp_pit) - float(my_pit)

        cycle_gain = 0.0
        used = False
        for value in [gain_track_norm, gain_stand]:
            if value is not None and pd.notna(value):
                cycle_gain += float(value)
                used = True
        if used:
            cumulative += cycle_gain
        else:
            cycle_gain = None

        rows.append({
            "Relais": relay,
            "Tours 96": my_laps_count,
            "Tours concurrent": cp_laps_count,
            "Tours comparables": comparable_laps,
            "Moyenne 96": seconds_to_laptime(my_avg),
            "Moyenne concurrent": seconds_to_laptime(cp_avg),
            "Gain piste normalisé sec": round(gain_track_norm, 3) if gain_track_norm is not None else None,
            "Gain piste normalisé": signed_seconds(gain_track_norm),
            "Delta piste brut sec": round(raw_track_delta, 3) if raw_track_delta is not None else None,
            "Arrêt 96": seconds_to_laptime(my_pit),
            "Arrêt concurrent": seconds_to_laptime(cp_pit),
            "Gain stand sec": round(gain_stand, 3) if gain_stand is not None else None,
            "Gain stand": signed_seconds(gain_stand),
            "Bilan relais sec": round(cycle_gain, 3) if cycle_gain is not None else None,
            "Bilan relais": signed_seconds(cycle_gain),
            "Cumul sec": round(cumulative, 3),
            "Cumul": signed_seconds(cumulative),
        })
    return pd.DataFrame(rows)


def dataframe_to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> Optional[bytes]:
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            for name, df in sheets.items():
                safe_name = name[:31]
                df.to_excel(writer, index=False, sheet_name=safe_name)
        output.seek(0)
        return output.getvalue()
    except Exception:
        return None


def highlight_our_bike(row: pd.Series) -> List[str]:
    try:
        if str(row.get("No.", "")) == MY_BIKE:
            return ["background-color:#153b64;color:white"] * len(row)
    except Exception:
        pass
    return [""] * len(row)

# ============================================================
# CHARGEMENT LIVE
# ============================================================

data = fetch_live_data()
if data is not None:
    save_snapshot_to_db(data)
    df_live = payload_to_dataframe(data)
    process_live_state(df_live)
else:
    df_live = pd.DataFrame()

# ============================================================
# NAVIGATION
# ============================================================

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choisir une page",
    [
        "Dashboard",
        "Live Timing",
        "Notre moto",
        "Pilotes",
        "Concurrents",
        "Détail concurrent",
        "Comparatif 96 vs concurrent",
        "Stratégie",
        "Historique & exports",
        "Paramètres",
    ],
)

st.title("Race Engineer - Endurance Moto")

# ============================================================
# PAGE DASHBOARD
# ============================================================

if page == "Dashboard":
    st.header("Tableau de bord")
    if data:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Session", data.get("Titre", "-"))
        c2.metric("Temps écoulé", format_duration_ms(data.get("TempsEcoule")))
        c3.metric("Temps restant", format_duration_ms(data.get("TempsRestant")))
        c4.metric("Heure UTC", data.get("HeureJourUTC", "-"))

    metrics = calculate_bike_metrics(df_live, MY_BIKE)
    if metrics is None:
        st.warning(f"Moto {MY_BIKE} non trouvee.")
    else:
        b = metrics["row"]
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Position", b.get("Pos.", "-"))
        col2.metric("Position catégorie", b.get("Cat.P", "-"))
        col3.metric("Tours", b.get("Laps", "-"))
        col4.metric("Dernier tour", clean_text(b.get("L. Lap", "-")))
        col5.metric("Meilleur tour", b.get("Best Lap", "-"))
        col6.metric("Prochain pit estimé", f"{metrics['laps_remaining']} tours")

        st.subheader("Carburant / pneus / freins")
        a, bcol, ccol = st.columns(3)
        with a:
            st.write(f"Carburant : **{metrics['fuel_remaining']:.1f} L / {CONFIG.get('fuel_capacity_l', 24.0):.1f} L**")
            st.progress(min(max(metrics["fuel_remaining"] / float(CONFIG.get("fuel_capacity_l", 24.0)), 0), 1))
            st.write(f"Pit window : **{metrics['pit_window']} tours**")
        with bcol:
            st.write(f"Pneus : **{metrics['tyre_remaining']} tours restants estimés**")
            st.progress(metrics["tyre_pct"])
            st.write(f"Fenêtre pneus : **{metrics['tyre_window']} tours**")
        with ccol:
            st.write(f"Freins : **{metrics['brake_remaining']} tours restants estimés**")
            st.progress(metrics["brake_pct"])

    st.subheader("Messages course")
    messages = data.get("Messages", []) if data else []
    if messages:
        st.write(" | ".join(messages[-5:]))
    else:
        st.info("Aucun message course disponible.")

# ============================================================
# PAGE LIVE TIMING
# ============================================================

elif page == "Live Timing":
    st.header("Live Timing")
    if df_live.empty:
        st.error("Impossible de recuperer les donnees")
    else:
        categorie = st.selectbox("Filtrer catégorie", ["ALL", "EWC", "SST", "PRD", "EXP"], key="live_category")
        df_display = df_live.copy()
        if categorie != "ALL":
            df_display = df_display[df_display["Cat"] == categorie]
        st.dataframe(df_display.style.apply(highlight_our_bike, axis=1), use_container_width=True, height=700)
        st.write(f"Nombre de motos affichées : {len(df_display)}")

# ============================================================
# PAGE NOTRE MOTO
# ============================================================

elif page == "Notre moto":
    st.header("Notre moto")
    metrics = calculate_bike_metrics(df_live, MY_BIKE)
    summary_data = bike_lap_summary(MY_BIKE)

    if metrics is None:
        st.warning(f"Moto {MY_BIKE} non trouvee.")
    else:
        row = metrics["row"]
        c1, c2, c3 = st.columns(3)
        c1.metric("Équipe", row.get("Team", "-"))
        c2.metric("Pilote actuel", clean_text(row.get("Rider", "-")))
        c3.metric("Catégorie", row.get("Cat", "-"))

        c4, c5, c6, c7 = st.columns(4)
        c4.metric("Tours", row.get("Laps", "-"))
        c5.metric("Dernier tour", clean_text(row.get("L. Lap", "-")))
        c6.metric("Meilleur tour", row.get("Best Lap", "-"))
        c7.metric("Last Pit", row.get("Last Pit", "-"))

    st.subheader("Statistiques moto enregistrées")
    summary = summary_data["summary"]
    if not summary:
        st.info("Aucun tour enregistré pour notre moto depuis le lancement de l'application.")
    else:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Tours enregistrés", summary.get("tours_enregistres", 0))
        c2.metric("Meilleur", seconds_to_laptime(summary.get("meilleur_tour")))
        c3.metric("Moyenne", seconds_to_laptime(summary.get("moyenne")))
        c4.metric("Médiane", seconds_to_laptime(summary.get("mediane")))
        c5.metric("Pace 5 meilleurs", seconds_to_laptime(summary.get("pace_5_best")))
        st.dataframe(summary_data["relay_summary"], use_container_width=True)

# ============================================================
# PAGE PILOTES
# ============================================================

elif page == "Pilotes":
    st.header("Pilotes")
    current_row = get_current_bike_row(df_live, MY_BIKE)
    current_rider = clean_text(current_row.get("Rider", "")).replace("*", "").strip() if current_row is not None else ""
    drivers = sorted(CONFIG.get("drivers", []), key=lambda x: x.get("order", 99))
    laps_df = load_lap_events_from_db(MY_BIKE)

    st.subheader("Statut pilotes configurés")
    if drivers:
        cards = st.columns(len(drivers))
        for idx, driver in enumerate(drivers):
            with cards[idx]:
                name = driver.get("name", "")
                color = driver.get("color", "#ffffff")
                is_current = bool(current_rider and (name.lower() in current_rider.lower() or current_rider.lower() in name.lower()))
                st.markdown(
                    f"""
                    <div style="border:1px solid #333;border-radius:12px;padding:14px;background:#111827;">
                        <div style="font-size:20px;font-weight:700;"><span style="color:{color};">●</span> {name}</div>
                        <div>Brassard : <b>{driver.get('armband', '')}</b></div>
                        <div>Ordre : <b>{driver.get('order', '')}</b></div>
                        <div>Statut : <b>{'EN RELAIS' if is_current else 'Disponible'}</b></div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    else:
        st.info("Aucun pilote configuré.")

    st.subheader("Tableau comparatif pilotes")
    if laps_df.empty:
        st.info("Aucun tour pilote enregistré.")
    else:
        rows = []
        for driver in drivers:
            name = driver.get("name", "")
            stats = driver_stats_for_name(name, laps_df)
            if not stats:
                stats = {"Pilote": name, "Tours total": 0, "Relais total": 0}
            stats["Pilote"] = name
            stats["Brassard"] = driver.get("armband", "")
            rows.append(stats)
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        selected_driver = st.selectbox("Détail pilote", [d.get("name", "") for d in drivers])
        stats = driver_stats_for_name(selected_driver, laps_df)
        st.write("Statistiques détaillées", stats if stats else "Pas de donnees pour ce pilote.")

        if px is not None:
            mask = laps_df["pilote"].fillna("").str.lower().str.contains(selected_driver.lower(), regex=False)
            df_driver = laps_df[mask].copy()
            if not df_driver.empty:
                fig = px.line(df_driver, x="tour_total", y="lap_seconds", color="relay_no", markers=True, title=f"Évolution des chronos - {selected_driver}")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.line_chart(laps_df.set_index("tour_total")[["lap_seconds"]])

# ============================================================
# PAGE CONCURRENTS
# ============================================================

elif page == "Concurrents":
    st.header("Concurrents - Relais et arrêts")
    categorie = st.selectbox("Catégorie à surveiller", ["PRD", "EWC", "SST", "EXP", "ALL"], key="competitor_category")

    st.subheader("Classement live filtré")
    if df_live.empty:
        st.warning("Pas de donnees live.")
    else:
        df_comp = df_live.copy()
        if categorie != "ALL":
            df_comp = df_comp[df_comp["Cat"] == categorie]
        useful_cols = ["Pos.", "Cat.P", "No.", "Cat", "Team", "Rider", "Laps", "L. Lap", "Best Lap", "Last Pit", "Last Pit Time", "Total Pit", "Total pit time"]
        useful_cols = [c for c in useful_cols if c in df_comp.columns]
        st.dataframe(df_comp[useful_cols].style.apply(highlight_our_bike, axis=1), use_container_width=True, height=350)

    st.subheader("Nombre de tours par relais")
    relay_matrix = build_relay_matrix(categorie)
    if relay_matrix.empty:
        st.info("Aucun relais détecté depuis le lancement. Le tableau se remplira au prochain arrêt détecté.")
    else:
        st.dataframe(relay_matrix, use_container_width=True)

    st.subheader("Temps de chaque arrêt - Last Pit Time")
    pit_matrix = build_pit_time_matrix(categorie)
    if pit_matrix.empty:
        st.info("Aucun temps d'arrêt enregistré pour l'instant.")
    else:
        st.dataframe(pit_matrix, use_container_width=True)

    st.subheader("Historique brut des relais détectés")
    events = load_relay_events_from_db()
    if not events.empty:
        if categorie != "ALL":
            events = events[events["categorie"] == categorie]
        st.dataframe(events, use_container_width=True)
    else:
        st.info("Historique vide pour l'instant.")

# ============================================================
# PAGE DETAIL CONCURRENT
# ============================================================

elif page == "Détail concurrent":
    st.header("Détail concurrent")
    if df_live.empty:
        st.warning("Pas de donnees live.")
    else:
        df_select = df_live.copy()
        categorie = st.selectbox("Filtrer catégorie", ["PRD", "EWC", "SST", "EXP", "ALL"], key="detail_cat")
        if categorie != "ALL":
            df_select = df_select[df_select["Cat"] == categorie]
        options = [f"{r['No.']} - {r['Team']}" for _, r in df_select.iterrows()]
        if options:
            selected = st.selectbox("Concurrent", options)
            numero = selected.split(" - ")[0]
            metrics = calculate_bike_metrics(df_live, numero)
            stats = bike_lap_summary(numero)
            if metrics:
                row = metrics["row"]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Position", row.get("Pos.", "-"))
                c2.metric("Cat.P", row.get("Cat.P", "-"))
                c3.metric("Tours", row.get("Laps", "-"))
                c4.metric("Last Pit", row.get("Last Pit", "-"))
            st.subheader("Relais enregistrés")
            st.dataframe(stats["relay_summary"], use_container_width=True)
            st.subheader("Tours enregistrés")
            st.dataframe(stats["laps_df"], use_container_width=True, height=350)

# ============================================================
# PAGE COMPARATIF
# ============================================================

elif page == "Comparatif 96 vs concurrent":
    st.header("Comparatif notre moto vs concurrent")
    st.caption("Valeurs positives = temps gagné par notre moto. Valeurs négatives = temps perdu.")

    if df_live.empty:
        st.warning("Pas de donnees live.")
    else:
        category = st.selectbox("Catégorie", ["PRD", "EWC", "SST", "EXP", "ALL"], key="comp_cat")
        df_options = df_live.copy()
        if category != "ALL":
            df_options = df_options[df_options["Cat"] == category]
        df_options = df_options[df_options["No."].astype(str) != MY_BIKE]
        options = [f"{r['No.']} - {r['Team']}" for _, r in df_options.iterrows()]
        if not options:
            st.info("Aucun concurrent disponible dans cette catégorie.")
        else:
            competitor = st.selectbox("Concurrent à comparer", options)
            competitor_numero = competitor.split(" - ")[0]
            comp_df = build_comparison(MY_BIKE, competitor_numero)
            if comp_df.empty:
                st.info("Pas encore assez de tours/relais enregistrés pour comparer.")
            else:
                st.dataframe(comp_df, use_container_width=True)
                c1, c2, c3 = st.columns(3)
                c1.metric("Gain piste cumulé", signed_seconds(comp_df["Gain piste normalisé sec"].dropna().sum() if "Gain piste normalisé sec" in comp_df else None))
                c2.metric("Gain stand cumulé", signed_seconds(comp_df["Gain stand sec"].dropna().sum() if "Gain stand sec" in comp_df else None))
                c3.metric("Bilan total", signed_seconds(comp_df["Bilan relais sec"].dropna().sum() if "Bilan relais sec" in comp_df else None))

                if px is not None:
                    chart_df = comp_df[["Relais", "Gain piste normalisé sec", "Gain stand sec", "Bilan relais sec", "Cumul sec"]].copy()
                    fig = px.bar(chart_df, x="Relais", y=["Gain piste normalisé sec", "Gain stand sec"], barmode="group", title="Gain/perte par relais")
                    st.plotly_chart(fig, use_container_width=True)
                    fig2 = px.line(chart_df, x="Relais", y="Cumul sec", markers=True, title="Bilan cumulé")
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.line_chart(comp_df.set_index("Relais")[["Cumul sec"]])

# ============================================================
# PAGE STRATEGIE
# ============================================================

elif page == "Stratégie":
    st.header("Stratégie")
    metrics = calculate_bike_metrics(df_live, MY_BIKE)
    if metrics is None:
        st.warning("Pas assez de donnees pour calculer la stratégie.")
    else:
        row = metrics["row"]
        current_lap = to_int(row.get("Laps")) or 0
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Carburant restant", f"{metrics['fuel_remaining']:.1f} L")
        c2.metric("Tours restants", metrics["laps_remaining"])
        c3.metric("Pit window", metrics["pit_window"])
        c4.metric("Pneus restants", metrics["tyre_remaining"])

        st.subheader("Programmer un événement stratégique")
        with st.form("strategy_event_form"):
            a, b, c, d = st.columns([1, 2, 2, 3])
            with a:
                event_lap = st.number_input("Tour", min_value=current_lap, max_value=current_lap + 200, value=current_lap + max(metrics["laps_remaining"], 1), step=1)
            with b:
                event_type = st.selectbox("Type", EVENT_TYPES)
            with c:
                driver = st.selectbox("Pilote", [""] + [d.get("name", "") for d in CONFIG.get("drivers", [])])
            with d:
                note = st.text_input("Note")
            submitted = st.form_submit_button("Ajouter")
        if submitted:
            save_strategy_event(int(event_lap), event_type, driver, note)
            st.rerun()
        if st.button("Effacer tous les événements planifiés"):
            clear_strategy_events()
            st.rerun()

        st.subheader("Timeline stratégique")
        timeline = build_strategy_timeline(current_lap, future_laps=60)
        st.dataframe(timeline.style.apply(color_timeline, axis=1), use_container_width=True, height=500)

        st.subheader("Événements planifiés")
        events = load_strategy_events()
        if events.empty:
            st.info("Aucun événement planifié.")
        else:
            st.dataframe(events, use_container_width=True)

# ============================================================
# PAGE HISTORIQUE ET EXPORTS
# ============================================================

elif page == "Historique & exports":
    st.header("Historique & exports")

    tabs = st.tabs(["Replay snapshots", "Exports", "Données brutes"])

    with tabs[0]:
        snapshots = load_snapshots_list(500)
        if snapshots.empty:
            st.info("Aucun snapshot enregistré.")
        else:
            labels = [f"{r['id']} - {r['fetched_at']} - {r.get('event_time_utc', '')}" for _, r in snapshots.iterrows()]
            selected = st.selectbox("Snapshot", labels)
            snapshot_id = int(selected.split(" - ")[0])
            payload = load_snapshot_payload(snapshot_id)
            if payload:
                df_snapshot = payload_to_dataframe(payload)
                cat = st.selectbox("Filtre catégorie replay", ["ALL", "PRD", "EWC", "SST", "EXP"])
                if cat != "ALL":
                    df_snapshot = df_snapshot[df_snapshot["Cat"] == cat]
                st.dataframe(df_snapshot, use_container_width=True, height=550)

    with tabs[1]:
        relay_df = load_relay_events_from_db()
        laps_df = load_lap_events_from_db()
        strategy_df = load_strategy_events()
        st.download_button("Télécharger relais CSV", relay_df.to_csv(index=False).encode("utf-8"), "relay_events.csv", "text/csv")
        st.download_button("Télécharger tours CSV", laps_df.to_csv(index=False).encode("utf-8"), "lap_events.csv", "text/csv")
        excel_bytes = dataframe_to_excel_bytes({"Relais": relay_df, "Tours": laps_df, "Strategie": strategy_df})
        if excel_bytes:
            st.download_button("Télécharger Excel complet", excel_bytes, "race_engineer_export.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("Export Excel indisponible. Ajoute openpyxl dans requirements.txt.")

    with tabs[2]:
        st.subheader("Relais")
        st.dataframe(load_relay_events_from_db(), use_container_width=True)
        st.subheader("Tours")
        st.dataframe(load_lap_events_from_db(), use_container_width=True)

# ============================================================
# PAGE PARAMETRES
# ============================================================

elif page == "Paramètres":
    st.header("Paramètres")
    with st.form("settings_form"):
        st.subheader("Source et équipe")
        live_url = st.text_input("URL live timing JSON", value=CONFIG.get("live_url", DEFAULT_LIVE_URL))
        my_bike = st.text_input("Numéro de notre moto", value=str(CONFIG.get("my_bike", "96")))
        team_name = st.text_input("Nom de l'équipe", value=CONFIG.get("team_name", "LEGACY COMPETITION"))
        category = st.selectbox("Catégorie", CATEGORIES, index=CATEGORIES.index(CONFIG.get("category", "PRD")) if CONFIG.get("category", "PRD") in CATEGORIES else 0)
        refresh_ms = st.number_input("Rafraîchissement live (millisecondes)", min_value=1000, max_value=30000, value=int(CONFIG.get("refresh_ms", 1000)), step=1000)
        save_every_snapshot = st.checkbox("Sauvegarder tous les snapshots", value=bool(CONFIG.get("save_every_snapshot", True)))

        st.subheader("Carburant")
        fuel_capacity_l = st.number_input("Capacité réservoir estimée (L)", value=float(CONFIG.get("fuel_capacity_l", 24.0)), step=0.5)
        fuel_fill_l = st.number_input("Quantité remise au plein (L)", value=float(CONFIG.get("fuel_fill_l", 24.0)), step=0.5)
        fuel_per_lap_l = st.number_input("Consommation estimée (L/tour)", value=float(CONFIG.get("fuel_per_lap_l", 0.70)), step=0.01)
        fuel_safety_laps = st.number_input("Marge sécurité carburant (tours)", value=int(CONFIG.get("fuel_safety_laps", 2)), step=1)

        st.subheader("Pneus et freins")
        tyre_tracking_enabled = st.checkbox("Activer suivi pneus", value=bool(CONFIG.get("tyre_tracking_enabled", True)))
        tyre_life_laps = st.number_input("Durée pneus estimée (tours)", value=int(CONFIG.get("tyre_life_laps", 45)), step=1)
        tyre_safety_laps = st.number_input("Marge sécurité pneus (tours)", value=int(CONFIG.get("tyre_safety_laps", 5)), step=1)
        brake_tracking_enabled = st.checkbox("Activer suivi freins", value=bool(CONFIG.get("brake_tracking_enabled", False)))
        brake_life_laps = st.number_input("Durée freins estimée (tours)", value=int(CONFIG.get("brake_life_laps", 600)), step=10)

        st.subheader("Pilotes / brassards")
        current_drivers = list(CONFIG.get("drivers", DEFAULT_CONFIG["drivers"]))
        driver_count = st.selectbox("Nombre de pilotes", [3, 4], index=0 if len(current_drivers) <= 3 else 1)
        while len(current_drivers) < driver_count:
            current_drivers.append({"name": f"Pilote {len(current_drivers) + 1}", "armband": "", "color": "#ffffff", "order": len(current_drivers) + 1, "active": True})
        new_drivers = []
        for i in range(driver_count):
            d = current_drivers[i]
            st.markdown(f"### Pilote {i + 1}")
            c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
            with c1:
                name = st.text_input(f"Nom pilote {i + 1}", value=d.get("name", ""), key=f"driver_name_{i}")
            with c2:
                armband = st.text_input(f"Brassard {i + 1}", value=d.get("armband", ""), key=f"driver_armband_{i}")
            with c3:
                color = st.color_picker(f"Couleur {i + 1}", value=d.get("color", "#ffffff"), key=f"driver_color_{i}")
            with c4:
                order = st.number_input(f"Ordre {i + 1}", min_value=1, max_value=4, value=int(d.get("order", i + 1)), step=1, key=f"driver_order_{i}")
            active = st.checkbox(f"Pilote {i + 1} actif", value=bool(d.get("active", True)), key=f"driver_active_{i}")
            new_drivers.append({"name": name, "armband": armband, "color": color, "order": int(order), "active": active})

        submitted = st.form_submit_button("Enregistrer les paramètres")

    if submitted:
        CONFIG.update({
            "live_url": live_url,
            "my_bike": str(my_bike),
            "team_name": team_name,
            "category": category,
            "refresh_ms": int(refresh_ms),
            "save_every_snapshot": bool(save_every_snapshot),
            "fuel_capacity_l": float(fuel_capacity_l),
            "fuel_fill_l": float(fuel_fill_l),
            "fuel_per_lap_l": float(fuel_per_lap_l),
            "fuel_safety_laps": int(fuel_safety_laps),
            "tyre_tracking_enabled": bool(tyre_tracking_enabled),
            "tyre_life_laps": int(tyre_life_laps),
            "tyre_safety_laps": int(tyre_safety_laps),
            "brake_tracking_enabled": bool(brake_tracking_enabled),
            "brake_life_laps": int(brake_life_laps),
            "drivers": sorted(new_drivers, key=lambda x: x["order"]),
        })
        st.session_state.config = CONFIG
        save_config(CONFIG)
        st.success("Paramètres enregistrés.")
        st.rerun()
