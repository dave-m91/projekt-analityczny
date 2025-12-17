import streamlit as st
import logging
import pandas as pd

if "base_url" not in st.session_state:
    st.session_state["base_url"] = "https://probable-goggles-jw5rvx96px4cp977-8000.app.github.dev/"

logging.basicConfig(
    filename="football_app.log",
    level=logging.INFO,
)
st.set_page_config(page_title = "Aplikacja Fantasy Football", page_icon = ":material/sports_football:")

page_1 = st.Page("page1.py", title="Skład drużyn", icon=":material/trophy:")
page_2 = st.Page("page2.py", title="Statystyki drużyn", icon=":material/star_border:")

pg = st.navigation([page_1, page_2])
pg.run()