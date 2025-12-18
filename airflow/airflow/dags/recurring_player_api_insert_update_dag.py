import datetime
import logging
from airflow.decorators import dag
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from shared_functions import upsert_player_data, upsert_team_data, upsert_league_data

def health_check_response(response):
    logging.info(f"Kod stanu odpowiedzi: {response.status_code}")
    logging.info(f"Treść odpowiedzi: {response.text}")
    return response.status_code == 200 and response.json() == {
        "message": "API health check successful"
    }

def insert_update_player_data(**context):
    player_json = context["ti"].xcom_pull(task_ids="api_player_query")

    if player_json:
        upsert_player_data(player_json)
    else:
        logging.warning("nie znaleziono zawodnika.")

def insert_update_team_data(**context):
    team_json = context["ti"].xcom_pull(task_ids = "api_team_query")

    if team_json:
        upsert_team_data(team_json)
    else:
        logging.warning("Nie znaleziono drużyny")

def insert_update_league_data(**context):
    league_json = context["ti"].xcom_pull(task_ids = "api_league_query")

    if league_json:
        upsert_league_data(league_json)
    else:
        logging.warning("Nie znaleziono ligi")

@dag(schedule = None)
def recurring_sport_api_insert_update_dag():
    api_health_check_task = HttpOperator(
        task_id = "check_api_health_check_endpoint",
        http_conn_id = "sportsworldcentral_url",
        endpoint = "/",
        method = "GET",
        headers = {"Content-Type": "application/json"},
        response_check = health_check_response
    )
    #temp_min_last_changed_date = "2024-04-01"

    api_player_query_task = HttpOperator(
        task_id = "api_player_query",
        http_conn_id = "sportsworldcentral_url",
        endpoint = ("/v0/players/?skip=0&limit=100000&minimum_last_changed_date={{ ds }}"),
        method = "GET",
        headers = {"Content-Type": "application/json"}
    )

    player_sqlite_upsert_task = PythonOperator(
        task_id = "player_sqlite_upsert",
        python_callable = insert_update_player_data
    )

    api_team_query_task = HttpOperator(
        task_id = "api_team_query",
        http_conn_id = "sportsworldcentral_url",
        endpoint = ("/v0/teams/?skip=0&limit=1000&minimum_last_changed_date={{ ds }}"),
        method = "GET",
        headers = {"Content-Type": "application/json"}
    )

    team_sqlite_upsert_task = PythonOperator(
        task_id = "team_sqlite_upsert",
        python_callable = insert_update_team_data
    )

    api_league_query_task = HttpOperator(
        task_id = "api_league_query",
        http_conn_id = "sportsworldcentral_url",
        endpoint = ("/v0/leagues/?skip=0&limit=1000&minimum_last_changed_date={{ ds }}"),
        method="GET",
        headers = {"Content-Type": "application/json"}
    )

    league_sqlite_upsert_task = PythonOperator(
        task_id = "league_sqlite_query",
        python_callable = insert_update_league_data
    )

    api_health_check_task >> [api_player_query_task, api_team_query_task, api_league_query_task]
    api_player_query_task >> player_sqlite_upsert_task
    api_team_query_task >> team_sqlite_upsert_task
    api_league_query_task >> league_sqlite_upsert_task

dag_instance = recurring_sport_api_insert_update_dag()