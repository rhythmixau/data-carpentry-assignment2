
import streamlit as st

st.set_page_config(layout="wide")
"""
# Data Carpentry  - Assignment 2
##### by Jack Toke
"""

pg = st.navigation({
    "Home": [
        st.Page("question1.py", title="Question 1"),
        st.Page("question2.py", title="Question 2"),
        st.Page("question3.py", title="Question 3"),
    ]
})

pg.run()

