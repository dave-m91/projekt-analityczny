import logging
import json
from airflow.hooks.base import BaseHook

def upsert_player_data(player_json):
    import sqlite3
    import pandas as pd
    #obiekt połączenia
    database_conn_id = "analytics_database"
    connection = BaseHook.get_connection(database_conn_id)
    sqlite_db_path = connection.schema
    if player_json:
        player_data = json.loads(player_json)
        #menadzer kontekstu
        with sqlite3.connect(sqlite_db_path) as conn:
            cursor = conn.cursor()
            for player in player_data:
                try:
                    cursor.execute("""
                                   INSERT INTO player (
                                   player_id,
                                   gsis_id,
                                   first_name,
                                   last_name,
                                   position,
                                   last_changed_date)
                                   VALUES (?, ?, ?, ?, ?, ?)
                                   ON CONFLICT(player_id) DO UPDATE
                                   SET gsis_id = excluded.gsis_id,
                                   first_name = excluded.first_name,
                                   last_name = excluded.last_name,
                                   position = excluded.position,
                                   last_changed_date = excluded.last_changed_date""",
                                   (player["player_id"],
                                    player["gsis_id"],
                                    player["first_name"],
                                    player["last_name"],
                                    player["position"],
                                    player["last_changed_date"]))
                except Exception as e:
                    logging.error(f"Nie udało się wstawić danych zawodnika {player['player_id']}: {e}")
                    raise
    else:
        logging.warning("Nie znaleziono danych zawodnika")
        raise ValueError("nie znaleziono danych gracza. Zadanie nie powiodło się z powodu braku dancyh")
    

def upsert_team_data(team_json):
    import sqlite3
    import pandas as pd
    database_conn_id = "analytics_database"
    connection = BaseHook.get_connection(database_conn_id)
    sqlite_db_path = connection.schema
    if team_json:
        team_data = json.loads(team_json)
        with sqlite3.connect(sqlite_db_path) as conn:
            cursor = conn.cursor()
            for team in team_data:
                try:
                    cursor.execute("""
                                    INSERT INTO team (
                                   league_id,
                                   team_id,
                                   team_name,
                                   last_changed_date)
                                   VALUES (?, ?, ?, ?)
                                   ON CONFLICT(league_id) DO UPDATE
                                   SET team_id excluded.team_id,
                                   team_name excluded.team_name,
                                   last_changed_date excluded.last_changed_date""",
                                   (team["league_id"],
                                    team["team_id"],
                                    team["team_name"],
                                    team["last_changed_date"]))
                except Exception as e:
                    logging.error(f"Nie udało się wstawić danych drużyny {team['team_id']}: {e}")
                    raise
    else:
        logging.warning("Nie znaleziono danych drużyny")
        raise ValueError("Nie znaleziono danych drużyny. Zadanie nie powiodło się z powodu braku danych")
