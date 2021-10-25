"""
Streamlit application entrypoint
"""
import streamlit as st
from pandas import DataFrame

st.set_page_config(layout="wide")

@st.cache(ttl=600)
def schools() -> DataFrame:
    from from_raw import schools_lea_years
    return schools_lea_years()

@st.cache(ttl=600, persist=True)
def graduation() -> DataFrame:
    from from_raw import graduation_rates
    return graduation_rates()

@st.cache(ttl=600, persist=True)
def enrollment() -> DataFrame:
    from from_raw import enrollment_17_18
    return enrollment_17_18()