
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Race Engineer - Endurance Moto", layout="wide")

URL = "https://fimewc.live-frclassification.fr/r1.json"
DB_PATH = Path("race_engineer_history.sqlite")
CONFIG_PATH = Path("race_engineer_config.json")

DEFAULT_CONFIG = {
    "my_bike": "96",
    "team_name": "LEGACY COMPETITION",
    "category": "PRD",
    "refresh_ms": 1000,
    "fuel_capacity_l": 24.0,
    "fuel_per_lap_l": 0.70,
    "fuel_safety_laps": 2,
    "drivers": [
        {"name": "Pilote 1", "armband": "Rouge", "color": "#ff4b4b", "order": 1, "active": True},
        {"name": "Pilote 2", "armband": "Bleu", "color": "#4b8bff", "order": 2, "active": True},
        {"name": "Pilote 3", "armband": "Jaune", "color": "#ffd84b", "order": 3, "active": True},
    ],
}

if "bike_state" not in st.session_state:
    st.session_state.bike_state = {}

if "relay_history" not in st.session_state:
    st.session_state.relay_history = []

if "config" not in st.session_state:
    st.session_state.config = None

if "pilot_lap_history" not in st.session_state:
    st.session_state.pilot_lap_history = []


def load_config():
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config(config):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"Impossible de sauvegarder les paramètres : {e}")


if st.session_state.config is None:
    st.session_state.config = load_config()

CONFIG = st.session_state.config
MY_BIKE = str(CONFIG.get("my_bike", "96"))

st_autorefresh(interval=int(CONFIG.get("refresh_ms", 1000)), key="live_refresh")


def clean_text(value):
    if value is None:
        return ""
    value = str(value)
    value = re.sub(r"\{.*?\}", "", value)
    return value.strip()


def to_int(value):
    try:
        value = clean_text(value)
        value = value.replace("Lp.", "").replace("Lp", "").strip()
        if value in ["", "-"]:
            return None
        return int(value)
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
    minutes = int(seconds // 60)
    rest = seconds - minutes * 60
    return f"{minutes}:{rest:06.3f}"


def is_pit_in(value):
    return "Pit In" in clean_text(value)


def is_pit_out(value):
    return "Pit Out" in clean_text(value)


def fetch_live_data():
    try:
        ts = int(datetime.now().timestamp() * 1000)
        r = requests.get(f"{URL}?t={ts}", timeout=10)
        r.raise_for_status()
        text = r.content.decode("utf-8-sig")
        return json.loads(text)
    except Exception as e:
        st.error(f"Impossible de récupérer les données : {e}")
        return None


def payload_to_dataframe(data):
    noms = [c["Texte"] for c in data["Colonnes"]]
    df = pd.DataFrame(data["Donnees"], columns=noms)
    if "" in df.columns:
        df = df.rename(columns={"": "Etat"})
    return df


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
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
    """)
    conn.commit()
    return conn


def save_snapshot_to_db(data):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO snapshots (fetched_at, payload) VALUES (?, ?)",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), json.dumps(data, ensure_ascii=False)),
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
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["timestamp"], event["numero"], event["categorie"], event["team"],
            event["pilote"], event["relais"], event["tours_relais"],
            event["last_pit_time"], event["tour_total"], event["total_pit"],
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"Erreur sauvegarde relais : {e}")


def save_pilot_lap_to_db(event):
    try:
        conn = db_connect()
        conn.execute("""
            INSERT INTO pilot_laps (
                timestamp, numero, pilote, tour_total, lap_time,
                lap_seconds, relay_laps
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            event["timestamp"], event["numero"], event["pilote"], event["tour_total"],
            event["lap_time"], event["lap_seconds"], event["relay_laps"],
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass


def load_relay_events_from_db():
    conn = db_connect()
    df = pd.read_sql_query(
        "SELECT timestamp, numero, categorie, team, pilote, relais, tours_relais, last_pit_time, tour_total, total_pit FROM relay_events ORDER BY id ASC",
        conn,
    )
    conn.close()
    return df


def load_pilot_laps_from_db(numero=None):
    conn = db_connect()
    if numero:
        df = pd.read_sql_query(
            "SELECT timestamp, numero, pilote, tour_total, lap_time, lap_seconds, relay_laps FROM pilot_laps WHERE numero = ? ORDER BY id ASC",
            conn,
            params=(str(numero),),
        )
    else:
        df = pd.read_sql_query(
            "SELECT timestamp, numero, pilote, tour_total, lap_time, lap_seconds, relay_laps FROM pilot_laps ORDER BY id ASC",
            conn,
        )
    conn.close()
    return df


def process_relay_detection(df):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    required_cols = ["No.", "Cat", "Team", "Rider", "Laps", "Last Pit", "Last Pit Time", "Total Pit", "L. Lap"]

    for col in required_cols:
        if col not in df.columns:
            return

    for _, row in df.iterrows():
        number = clean_text(row["No."])
        if not number:
            continue

        category = clean_text(row["Cat"])
        team = clean_text(row["Team"])
        rider = clean_text(row["Rider"]).replace("*", "").strip()
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
                "previous_laps": laps,
                "last_recorded_lap": laps,
            }
            continue

        state = st.session_state.bike_state[number]
        previous_last_pit = state.get("previous_last_pit")
        previous_total_pit = state.get("previous_total_pit")
        last_recorded_total_pit = state.get("last_recorded_total_pit")
        last_recorded_lap = state.get("last_recorded_lap")

        stop_validated = False

        if (
            total_pit is not None
            and previous_total_pit is not None
            and total_pit > previous_total_pit
            and total_pit != last_recorded_total_pit
        ):
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
            if relay_laps is not None and relay_laps > 0:
                state["relay_number"] = state.get("relay_number", 0) + 1
                event = {
                    "timestamp": now,
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

        if number == MY_BIKE and laps is not None and last_lap_seconds is not None:
            if last_recorded_lap is not None and laps > last_recorded_lap:
                lap_event = {
                    "timestamp": now,
                    "numero": number,
                    "pilote": rider,
                    "tour_total": laps,
                    "lap_time": last_lap,
                    "lap_seconds": last_lap_seconds,
                    "relay_laps": last_pit,
                }
                st.session_state.pilot_lap_history.append(lap_event)
                save_pilot_lap_to_db(lap_event)
                state["last_recorded_lap"] = laps

        state["previous_last_pit"] = last_pit
        state["previous_total_pit"] = total_pit
        state["previous_laps"] = laps


def build_relay_matrix(category_filter="PRD"):
    df_hist = load_relay_events_from_db()
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
        aggfunc="last",
    ).reset_index()
    matrix.columns = [f"Relais N°{c}" if isinstance(c, int) else c for c in matrix.columns]
    return matrix


def build_pit_time_matrix(category_filter="PRD"):
    df_hist = load_relay_events_from_db()
    if df_hist.empty:
        return pd.DataFrame()
    if category_filter != "ALL":
        df_hist = df_hist[df_hist["categorie"] == category_filter]
    if df_hist.empty:
        return pd.DataFrame()

    matrix = df_hist.pivot_table(
        index=["numero", "team"],
        columns="relais",
        values="last_pit_time",
        aggfunc="last",
    ).reset_index()
    matrix.columns = [f"Arrêt N°{c}" if isinstance(c, int) else c for c in matrix.columns]
    return matrix


def get_current_bike_row(df_live):
    if df_live.empty or "No." not in df_live.columns:
        return None
    bike = df_live[df_live["No."].astype(str) == MY_BIKE]
    if bike.empty:
        return None
    return bike.iloc[0]


def calculate_bike_metrics(df_live):
    row = get_current_bike_row(df_live)
    if row is None:
        return None

    last_pit = to_int(row.get("Last Pit"))
    fuel_capacity = float(CONFIG.get("fuel_capacity_l", 24.0))
    fuel_per_lap = float(CONFIG.get("fuel_per_lap_l", 0.70))
    safety_laps = int(CONFIG.get("fuel_safety_laps", 2))

    fuel_used = (last_pit or 0) * fuel_per_lap
    fuel_remaining = max(fuel_capacity - fuel_used, 0)
    laps_remaining = int(fuel_remaining / fuel_per_lap) if fuel_per_lap > 0 else 0
    pit_window_min = max(laps_remaining - safety_laps, 0)

    return {
        "row": row,
        "fuel_remaining": fuel_remaining,
        "laps_remaining": laps_remaining,
        "pit_window": f"{pit_window_min}-{laps_remaining}",
    }


data = fetch_live_data()
if data is not None:
    save_snapshot_to_db(data)
    df_live = payload_to_dataframe(data)
    process_relay_detection(df_live)
else:
    df_live = pd.DataFrame()

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choisir une page",
    ["Dashboard", "Live Timing", "Notre moto", "Pilotes", "Concurrents", "Stratégie", "Paramètres"],
)

st.title("Race Engineer - Endurance Moto")

if page == "Dashboard":
    st.header("Tableau de bord")
    metrics = calculate_bike_metrics(df_live)

    if metrics is None:
        st.warning(f"Moto {MY_BIKE} non trouvée.")
    else:
        b = metrics["row"]
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Position", b.get("Pos.", "-"))
        col2.metric("Position catégorie", b.get("Cat.P", "-"))
        col3.metric("Tours", b.get("Laps", "-"))
        col4.metric("Dernier tour", clean_text(b.get("L. Lap", "-")))
        col5.metric("Meilleur tour", b.get("Best Lap", "-"))
        col6.metric("Prochain pit", f"{metrics['laps_remaining']} tours")

        st.subheader("Carburant estimé")
        ratio = metrics["fuel_remaining"] / float(CONFIG.get("fuel_capacity_l", 24.0))
        st.progress(min(max(ratio, 0), 1))
        st.write(
            f"Carburant estimé : **{metrics['fuel_remaining']:.1f} L / {CONFIG.get('fuel_capacity_l', 24.0):.1f} L** — "
            f"Pit window : **{metrics['pit_window']} tours**"
        )

        st.subheader("Notre moto")
        st.dataframe(pd.DataFrame([b]), use_container_width=True)

if page == "Live Timing":
    st.header("Live Timing")
    if df_live.empty:
        st.error("Impossible de récupérer les données")
    else:
        categorie = st.selectbox("Filtrer catégorie", ["ALL", "EWC", "SST", "PRD", "EXP"], key="live_category")
        df_display = df_live.copy()
        if categorie != "ALL":
            df_display = df_display[df_display["Cat"] == categorie]
        st.dataframe(df_display, use_container_width=True, height=650)
        st.write(f"Nombre de motos affichées : {len(df_display)}")

if page == "Notre moto":
    st.header("Notre moto")
    metrics = calculate_bike_metrics(df_live)

    if metrics is None:
        st.warning(f"Moto {MY_BIKE} non trouvée.")
    else:
        b = metrics["row"]
        col1, col2, col3 = st.columns(3)
        col1.metric("Équipe", b.get("Team", "-"))
        col2.metric("Pilote actuel", clean_text(b.get("Rider", "-")))
        col3.metric("Catégorie", b.get("Cat", "-"))

        col4, col5, col6, col7 = st.columns(4)
        col4.metric("Tours", b.get("Laps", "-"))
        col5.metric("Dernier tour", clean_text(b.get("L. Lap", "-")))
        col6.metric("Meilleur tour", b.get("Best Lap", "-"))
        col7.metric("Tours depuis dernier stand", b.get("Last Pit", "-"))

        st.subheader("Carburant")
        st.metric("Carburant restant estimé", f"{metrics['fuel_remaining']:.1f} L")
        st.metric("Tours restants estimés", metrics["laps_remaining"])
        st.metric("Pit window", metrics["pit_window"])

        st.subheader("Données complètes")
        st.dataframe(pd.DataFrame([b]), use_container_width=True)

if page == "Pilotes":
    st.header("Pilotes")
    current_row = get_current_bike_row(df_live)
    current_rider = clean_text(current_row.get("Rider", "")).replace("*", "").strip() if current_row is not None else ""
    drivers = sorted(CONFIG.get("drivers", []), key=lambda x: x.get("order", 99))

    st.subheader("Statut pilotes configurés")
    if not drivers:
        st.info("Aucun pilote configuré.")
    else:
        cards = st.columns(len(drivers))
        for idx, driver in enumerate(drivers):
            with cards[idx]:
                name = driver.get("name", "")
                color = driver.get("color", "#ffffff")
                is_current = bool(current_rider and (name.lower() in current_rider.lower() or current_rider.lower() in name.lower()))
                st.markdown(
                    f"""
                    <div style="border:1px solid #333;border-radius:12px;padding:14px;background:#111827;">
                        <div style="font-size:20px;font-weight:700;">
                            <span style="color:{color};">●</span> {name}
                        </div>
                        <div>Brassard : <b>{driver.get('armband', '')}</b></div>
                        <div>Ordre : <b>{driver.get('order', '')}</b></div>
                        <div>Statut : <b>{"EN RELAIS" if is_current else "Disponible"}</b></div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.subheader("Statistiques par pilote")
    laps_df = load_pilot_laps_from_db(MY_BIKE)
    if laps_df.empty:
        st.info("Aucun tour pilote enregistré depuis le lancement / la sauvegarde.")
    else:
        summary = []
        for pilote, group in laps_df.groupby("pilote"):
            valid = group.dropna(subset=["lap_seconds"])
            best = valid["lap_seconds"].min() if not valid.empty else None
            avg = valid["lap_seconds"].mean() if not valid.empty else None
            recent = valid.tail(10)["lap_seconds"].mean() if len(valid) else None
            summary.append({
                "Pilote": pilote,
                "Tours enregistrés": len(valid),
                "Meilleur tour": seconds_to_laptime(best),
                "Moyenne globale": seconds_to_laptime(avg),
                "Moyenne récente": seconds_to_laptime(recent),
            })
        st.dataframe(pd.DataFrame(summary), use_container_width=True)
        st.subheader("Historique tours pilote")
        st.dataframe(laps_df, use_container_width=True, height=350)

if page == "Concurrents":
    st.header("Concurrents - Relais et arrêts")
    categorie = st.selectbox("Catégorie à surveiller", ["PRD", "EWC", "SST", "EXP", "ALL"], key="competitor_category")

    st.subheader("Classement live filtré")
    if df_live.empty:
        st.warning("Pas de données live.")
    else:
        df_comp = df_live.copy()
        if categorie != "ALL":
            df_comp = df_comp[df_comp["Cat"] == categorie]
        useful_cols = [
            "Pos.", "Cat.P", "No.", "Cat", "Team", "Rider", "Laps", "L. Lap",
            "Best Lap", "Last Pit", "Last Pit Time", "Total Pit", "Total pit time",
        ]
        useful_cols = [c for c in useful_cols if c in df_comp.columns]
        st.dataframe(df_comp[useful_cols], use_container_width=True, height=350)

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
    df_events = load_relay_events_from_db()
    if df_events.empty:
        st.info("Historique vide pour l'instant.")
    else:
        if categorie != "ALL":
            df_events = df_events[df_events["categorie"] == categorie]
        st.dataframe(df_events, use_container_width=True)

if page == "Stratégie":
    st.header("Stratégie")
    metrics = calculate_bike_metrics(df_live)

    if metrics is None:
        st.warning("Pas assez de données pour calculer la stratégie.")
    else:
        st.subheader("Prédiction carburant simple")
        col1, col2, col3 = st.columns(3)
        col1.metric("Carburant restant estimé", f"{metrics['fuel_remaining']:.1f} L")
        col2.metric("Tours restants", metrics["laps_remaining"])
        col3.metric("Pit window", metrics["pit_window"])
        st.info("Prochaine étape : timeline stratégique avec pit, pneus, pilotes et scénarios.")

if page == "Paramètres":
    st.header("Paramètres")

    with st.form("settings_form"):
        st.subheader("Paramètres équipe")
        my_bike = st.text_input("Numéro de notre moto", value=str(CONFIG.get("my_bike", "96")))
        team_name = st.text_input("Nom de l'équipe", value=CONFIG.get("team_name", "LEGACY COMPETITION"))
        category = st.selectbox("Catégorie", ["PRD", "EWC", "SST", "EXP", "ALL"], index=["PRD", "EWC", "SST", "EXP", "ALL"].index(CONFIG.get("category", "PRD")))
        refresh_ms = st.number_input("Rafraîchissement live (millisecondes)", min_value=1000, max_value=30000, value=int(CONFIG.get("refresh_ms", 1000)), step=1000)

        st.subheader("Carburant")
        fuel_capacity_l = st.number_input("Capacité réservoir estimée (L)", value=float(CONFIG.get("fuel_capacity_l", 24.0)), step=0.5)
        fuel_per_lap_l = st.number_input("Consommation estimée (L/tour)", value=float(CONFIG.get("fuel_per_lap_l", 0.70)), step=0.01)
        fuel_safety_laps = st.number_input("Marge sécurité carburant (tours)", value=int(CONFIG.get("fuel_safety_laps", 2)), step=1)

        st.subheader("Pilotes / brassards")
        current_drivers = CONFIG.get("drivers", DEFAULT_CONFIG["drivers"])
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
            "my_bike": str(my_bike),
            "team_name": team_name,
            "category": category,
            "refresh_ms": int(refresh_ms),
            "fuel_capacity_l": float(fuel_capacity_l),
            "fuel_per_lap_l": float(fuel_per_lap_l),
            "fuel_safety_laps": int(fuel_safety_laps),
            "drivers": sorted(new_drivers, key=lambda x: x["order"]),
        })
        st.session_state.config = CONFIG
        save_config(CONFIG)
        st.success("Paramètres enregistrés.")
        st.rerun()
