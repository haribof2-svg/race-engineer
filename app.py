import json
import re
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Race Engineer", layout="wide")

URL = "https://fimewc.live-frclassification.fr/r1.json"
MY_BIKE = "96"
DB_PATH = Path("race_engineer_history.sqlite")

# Rafraîchissement automatique toutes les 1 seconde
st_autorefresh(interval=1000, key="live_refresh")

st.title("Race Engineer - Endurance Moto")

# -----------------------------
# SESSION STATE
# -----------------------------
if "bike_state" not in st.session_state:
    st.session_state.bike_state = {}

if "relay_history" not in st.session_state:
    st.session_state.relay_history = []

if "last_snapshot_time" not in st.session_state:
    st.session_state.last_snapshot_time = None


# -----------------------------
# OUTILS
# -----------------------------
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


def is_pit_in(value):
    return "Pit In" in clean_text(value)


def is_pit_out(value):
    return "Pit Out" in clean_text(value)

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
    conn.commit()
    return conn


def save_snapshot_to_db(data):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO snapshots (fetched_at, payload) VALUES (?, ?)",
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                json.dumps(data, ensure_ascii=False),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"Erreur sauvegarde snapshot : {e}")


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
            event["timestamp"],
            event["numero"],
            event["categorie"],
            event["team"],
            event["pilote"],
            event["relais"],
            event["tours_relais"],
            event["last_pit_time"],
            event["tour_total"],
            event["total_pit"],
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"Erreur sauvegarde relais : {e}")


def load_relay_events_from_db():
    conn = db_connect()
    df = pd.read_sql_query(
        "SELECT timestamp, numero, categorie, team, pilote, relais, tours_relais, last_pit_time, tour_total, total_pit FROM relay_events ORDER BY id ASC",
        conn,
    )
    conn.close()
    return df




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
    colonnes = data["Colonnes"]
    donnees = data["Donnees"]

    noms = [c["Texte"] for c in colonnes]
    df = pd.DataFrame(donnees, columns=noms)

    # Nettoyage noms vides
    if "" in df.columns:
        df = df.rename(columns={"": "Etat"})

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
        rider = clean_text(row["Rider"])
        laps = to_int(row["Laps"])
        last_pit = to_int(row["Last Pit"])
        last_pit_time = clean_text(row["Last Pit Time"])
        total_pit = to_int(row["Total Pit"])
        last_lap = clean_text(row["L. Lap"])

        if number not in st.session_state.bike_state:
            st.session_state.bike_state[number] = {
                "previous_last_pit": last_pit,
                "previous_total_pit": total_pit,
                "last_recorded_total_pit": total_pit,
                "relay_number": 0,
            }
            continue

        state = st.session_state.bike_state[number]

        previous_last_pit = state.get("previous_last_pit")
        previous_total_pit = state.get("previous_total_pit")
        last_recorded_total_pit = state.get("last_recorded_total_pit")

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

        state["previous_last_pit"] = last_pit
        state["previous_total_pit"] = total_pit

def build_relay_matrix(category_filter="PRD"):
    history = st.session_state.relay_history

    if category_filter != "ALL":
        history = [h for h in history if h["categorie"] == category_filter]

    if not history:
        return pd.DataFrame()

    df_hist = pd.DataFrame(history)

    matrix = df_hist.pivot_table(
        index=["numero", "team"],
        columns="relais",
        values="tours_relais",
        aggfunc="last"
    ).reset_index()

    matrix.columns = [
        f"Relais N°{c}" if isinstance(c, int) else c
        for c in matrix.columns
    ]

    return matrix


def build_pit_time_matrix(category_filter="PRD"):
    history = st.session_state.relay_history

    if category_filter != "ALL":
        history = [h for h in history if h["categorie"] == category_filter]

    if not history:
        return pd.DataFrame()

    df_hist = pd.DataFrame(history)

    matrix = df_hist.pivot_table(
        index=["numero", "team"],
        columns="relais",
        values="last_pit_time",
        aggfunc="last"
    ).reset_index()

    matrix.columns = [
        f"Arrêt N°{c}" if isinstance(c, int) else c
        for c in matrix.columns
    ]

    return matrix


# -----------------------------
# MENU
# -----------------------------
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choisir une page",
    [
        "Dashboard",
        "Live Timing",
        "Notre moto",
        "Pilotes",
        "Concurrents",
        "Stratégie",
        "Paramètres"
    ]
)

# -----------------------------
# DATA LIVE
# -----------------------------
data = fetch_live_data()

if data is not None:
    save_snapshot_to_db(data)
    df_live = payload_to_dataframe(data)
    process_relay_detection(df_live)
else:
    df_live = pd.DataFrame()

# -----------------------------
# PAGE DASHBOARD
# -----------------------------
if page == "Dashboard":
    st.header("Dashboard")

    if df_live.empty:
        st.warning("Pas de données live disponibles.")
    else:
        bike = df_live[df_live["No."] == MY_BIKE]

        if bike.empty:
            st.warning(f"Moto {MY_BIKE} non trouvée.")
        else:
            b = bike.iloc[0]

            col1, col2, col3, col4, col5 = st.columns(5)

            col1.metric("Position", b["Pos."])
            col2.metric("Position catégorie", b["Cat.P"])
            col3.metric("Tours", b["Laps"])
            col4.metric("Dernier tour", clean_text(b["L. Lap"]))
            col5.metric("Last Pit", b["Last Pit"])

            st.subheader("Notre moto")
            st.dataframe(bike, use_container_width=True)


# -----------------------------
# PAGE LIVE TIMING
# -----------------------------
if page == "Live Timing":
    st.header("Live Timing")

    if df_live.empty:
        st.error("Impossible de récupérer les données")
    else:
        categorie = st.selectbox(
            "Filtrer catégorie",
            ["ALL", "EWC", "SST", "PRD", "EXP"],
            key="live_category"
        )

        df_display = df_live.copy()

        if categorie != "ALL":
            df_display = df_display[df_display["Cat"] == categorie]

        st.dataframe(df_display, use_container_width=True, height=650)
        st.write(f"Nombre de motos affichées : {len(df_display)}")


# -----------------------------
# PAGE NOTRE MOTO
# -----------------------------
if page == "Notre moto":
    st.header("Notre moto")

    if df_live.empty:
        st.warning("Pas de données.")
    else:
        bike = df_live[df_live["No."] == MY_BIKE]

        if bike.empty:
            st.warning(f"Moto {MY_BIKE} non trouvée.")
        else:
            b = bike.iloc[0]

            col1, col2, col3 = st.columns(3)
            col1.metric("Équipe", b["Team"])
            col2.metric("Pilote actuel", b["Rider"])
            col3.metric("Catégorie", b["Cat"])

            col4, col5, col6, col7 = st.columns(4)
            col4.metric("Tours", b["Laps"])
            col5.metric("Dernier tour", clean_text(b["L. Lap"]))
            col6.metric("Meilleur tour", b["Best Lap"])
            col7.metric("Tours depuis dernier stand", b["Last Pit"])

            st.subheader("Données complètes")
            st.dataframe(bike, use_container_width=True)


# -----------------------------
# PAGE PILOTES
# -----------------------------
if page == "Pilotes":
    st.header("Pilotes")

    st.info(
        "Prochaine brique : page de paramétrage des pilotes, brassards, ordre de passage, "
        "puis attribution automatique des tours au pilote affiché dans le live timing."
    )


# -----------------------------
# PAGE CONCURRENTS
# -----------------------------
if page == "Concurrents":
    st.header("Concurrents - Relais et arrêts")

    categorie = st.selectbox(
        "Catégorie à surveiller",
        ["PRD", "EWC", "SST", "EXP", "ALL"],
        key="competitor_category"
    )

    st.subheader("Classement live filtré")

    if df_live.empty:
        st.warning("Pas de données live.")
    else:
        df_comp = df_live.copy()

        if categorie != "ALL":
            df_comp = df_comp[df_comp["Cat"] == categorie]

        useful_cols = [
            "Pos.", "Cat.P", "No.", "Cat", "Team", "Rider",
            "Laps", "L. Lap", "Best Lap", "Last Pit",
            "Last Pit Time", "Total Pit", "Total pit time"
        ]

        useful_cols = [c for c in useful_cols if c in df_comp.columns]
        st.dataframe(df_comp[useful_cols], use_container_width=True, height=350)

    st.subheader("Nombre de tours par relais")

    relay_matrix = build_relay_matrix(categorie)

    if relay_matrix.empty:
        st.info(
            "Aucun relais détecté depuis le lancement de l'application. "
            "Le tableau va se remplir automatiquement au prochain arrêt détecté."
        )
    else:
        st.dataframe(relay_matrix, use_container_width=True)

    st.subheader("Temps de chaque arrêt - Last Pit Time")

    pit_matrix = build_pit_time_matrix(categorie)

    if pit_matrix.empty:
        st.info("Aucun temps d'arrêt enregistré pour l'instant.")
    else:
        st.dataframe(pit_matrix, use_container_width=True)

    st.subheader("Historique brut des relais détectés")

df_db_events = load_relay_events_from_db()

if not df_db_events.empty:
    if categorie != "ALL":
        df_db_events = df_db_events[df_db_events["categorie"] == categorie]
    st.dataframe(df_db_events, use_container_width=True)
else:
    st.info("Historique vide pour l'instant.")


# -----------------------------
# PAGE STRATEGIE
# -----------------------------
if page == "Stratégie":
    st.header("Stratégie")

    st.info(
        "À venir : timeline stratégique, prédiction prochain pit, carburant, pneus, "
        "comparaison de scénarios."
    )


# -----------------------------
# PAGE PARAMETRES
# -----------------------------
if page == "Paramètres":
    st.header("Paramètres")

    st.subheader("Paramètres équipe")

    st.text_input("Numéro de notre moto", value=MY_BIKE)
    st.number_input("Capacité réservoir estimée (L)", value=24.0)
    st.number_input("Consommation estimée (L/tour)", value=0.70)
    st.number_input("Rafraîchissement live (secondes)", value=1)

    st.subheader("Pilotes")

    st.info(
        "Prochaine étape : rendre ces pilotes enregistrables avec nom, couleur de brassard, "
        "ordre de passage et nombre de pilotes 3 ou 4."
    )