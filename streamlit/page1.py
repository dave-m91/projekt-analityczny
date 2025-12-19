import streamlit as st
import swc_simple_client as swc
import pandas as pd
import logging

logger = logging.getLogger(__name__) # odwołanie do pliku logu

st.header("Aplikacja SportsWolrdCentral")
st.subheader("Składy drużyn")


base_url = st.session_state["base_url"]
# wdrożenie buforowania
@st.cache_data(ttl=600, show_spinner="Pobieranie danych z API...")
def get_teams_from_api(url, endpoint):
    logger.info(f"Pobieranie danych z: {url}{endpoint}")
    return swc.call_api_endpoint(url, endpoint)

try:
    team_api_response = get_teams_from_api(base_url, swc.LIST_TEAMS_ENDPOINT) # wywołanie api

    if team_api_response.status_code == 200:
        team_data = team_api_response.json()
        teams_df = pd.DataFrame.from_dict(team_data)

        unique_leagues = teams_df["league_id"].unique()
        unique_leagues = sorted(unique_leagues.astype(str))

        if "unique_leagues" not in st.session_state:
            st.session_state["unique_leagues"] = unique_leagues

        selected_league = st.sidebar.selectbox("Wybierz ID ligi", unique_leagues) # wybór wartości

        st.sidebar.divider()
        st.sidebar.subheader(":blue[Źródła danych]")
        st.sidebar.text("SportsWorldCentral")

        if st.sidebar.button("Odśwież dane"):
            st.cache_data.clear()
            st.rerun()

        flat_team_df = pd.json_normalize(
            team_data, "players", ["team_id", "team_name", "league_id"]
        )
        column_order = ["league_id", "team_id", "team_name", "position",
                        "player_id", "gsis_id", "first_name", "last_name"]
        flat_team_df_ordered = flat_team_df[column_order]

        if "flat_team_df_ordered" not in st.session_state:
            st.session_state["flat_team_df_ordered"] = flat_team_df_ordered

        display_df = flat_team_df_ordered.drop(columns=["team_id", "player_id"])

        display_df["league_id"] = display_df["league_id"].astype("str")
        display_df = display_df[display_df["league_id"] == selected_league]

        st.dataframe(display_df, hide_index=True)
    else:
        logger.error(f"Wystąpił błąd: {team_api_response.status_code} {team_api_response.text}")
        st.write("Wystąpił błąd podczas uzyskiwania dostępu do źródła danych")
except Exception as e:
    logger.error(f"Wystąpił wyjątek: {str(e)}")
    st.write(f"wystąpił nieoczekiwany błąd.")
