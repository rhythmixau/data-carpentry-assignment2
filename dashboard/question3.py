import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
from constants import DUCKDB_FILE

st.markdown("Q3 - How much has inflation impacted the profit margin of each business?")

periods = {
    1: "July 2022 - June 2023",
    2: "July 2023 - June 2024",
    3: "July 2024 - June 2025",
    4: "July 2025 - June 2026",
    5: "July 2026 - June 2027",
    6: "July 2027 - June 2028",
    7: "July 2028 - June 2029",
}

with duckdb.connect(DUCKDB_FILE) as conn:
    business_employees_df = conn.sql("""
    select * from business_employees
order by business_name, "year", "month", cashier_name
    """).df()

    business_employee_summary_df = conn.sql("""
    select business_name, year, month, month_name, num_employees from business_employee_stats
    """).df()

st.markdown("### Initial Employee Data")
st.dataframe(business_employees_df)
st.markdown("### Initial Employee Summary")
st.dataframe(business_employee_summary_df)
st.markdown("### Employee Leaving Stats")
st.markdown("It is going to be faster for me to complete this step manually, that is to count "
            "the number of employee who left during each period.")
business_employee_period_df = business_employee_summary_df[business_employee_summary_df["month"].isin([6, 7])].reset_index(drop=True)
business_employee_period_df["month-year"] = business_employee_period_df["month"].astype(str) + "-" +business_employee_period_df["year"].astype(str)

businesses = ["Ed's Barber Supplies", 'Penguin Swim School', 'Please Bring Pizza Pronto', 'Wake Up with Coffee']

employee_left_records = {
    "Ed's Barber Supplies": {
        "July 2022 - June 2023": 0,
        "July 2023 - June 2024": 1, # Anthony
        "July 2024 - June 2025": 1, # Kathryn
        "July 2025 - June 2026": 1, # Jonathon
        "July 2026 - June 2027": 0, #
        "July 2027 - June 2028": 0, # Eric, Scott, Todd
        "July 2028 - June 2029": 11 # Natasha, Justin, Jennifer, Olivia, Michael, Juan, Barbara, Carlos, Robert, Cheryl, Ebony
        },
    "Penguin Swim School": {
        "July 2022 - June 2023": 0,
        "July 2023 - June 2024": 1, # Theresa
        "July 2024 - June 2025": 0, #
        "July 2025 - June 2026": 1, # Jennifer
        "July 2026 - June 2027": 0, #
        "July 2027 - June 2028": 0, #
        "July 2028 - June 2029": 4 # Kevin, Joseph, Mary, Patricia
        },
    "Please Bring Pizza Pronto": {
        "July 2022 - June 2023": 1, # Christian
        "July 2023 - June 2024": 1, # August
        "July 2024 - June 2025": 1, # Tara
        "July 2025 - June 2026": 0, #
        "July 2026 - June 2027": 2, # Ronald, Steven
        "July 2027 - June 2028": 1, # Maurice
        "July 2028 - June 2029": 1 # Amber
        },
    "Wake Up with Coffee": {
        "July 2022 - June 2023": 0, #
        "July 2023 - June 2024": 0, #
        "July 2024 - June 2025": 0, #
        "July 2025 - June 2026": 2, # Leslie, Amber
        "July 2026 - June 2027": 1, # Mary, Matthew
        "July 2027 - June 2028": 1, # Scott
        "July 2028 - June 2029": 6  # Diane, Nathaniel, Lauren, Kelly, Brian, Lauren
        },
    }

with duckdb.connect(DUCKDB_FILE) as conn:
    ed_employee_att_records_df = conn.sql(f"SELECT * FROM employee_attendences").df()

final_stat = pd.DataFrame(columns=["business_name", "avg_turnover_rate", "median_turnover_rate",])

for business_name in businesses:
    ed_df = business_employee_period_df[business_employee_period_df["business_name"] == business_name].reset_index(drop=True)
    ed_df["period"] = (ed_df.index // 2) + 1
    ed_df["year_period"] = ed_df["period"].map(periods)
    ed_df = ed_df.pivot(index=["business_name", "year_period"], columns=["month_name"], values="num_employees")
    ed_df.reset_index(inplace=True)
    ed_df["annual_avg_employee"] = (ed_df["July"] + ed_df["June"])/2
    ed_df["num_employee_left"] = 0

    st.dataframe(ed_df)

    # for business_name in businesses:
    st.markdown(f"### {business_name} Active Employee Records")
    business_employee_attendances = ed_employee_att_records_df[ed_employee_att_records_df["business_name"] == business_name]
    st.dataframe(business_employee_attendances)
    ed_df["num_employee_left"] = ed_df["year_period"].map(employee_left_records[business_name])
    ed_df["annual_turnover_rate"] = (ed_df["num_employee_left"] / ed_df["annual_avg_employee"]) * 100
    st.markdown(f"### {business_name} Employee Turnover Rate")
    st.dataframe(ed_df)
    final_stat.loc[len(final_stat)] = [business_name, ed_df["annual_turnover_rate"].mean(), ed_df["annual_turnover_rate"].median()]

st.markdown("### Employee Turnover Rate Analysis")
st.dataframe(final_stat)
st.write("Employee turnover rate is a crucial metric for businesses to understand the stability of their workforce.")
st.write("It indicates the percentage of employees who leave the company within a specific period.")
st.write("Let's analyze the employee turnover rate for Ed's Barber Supplies.")

# Example Calculation
# Let's say you want to calculate the annual turnover rate for your company.
#
# You had 100 employees on January 1st.
#
# You had 120 employees on December 31st.
#
# During the year, 15 employees left the company.
#
# First, calculate the average number of employees:
#
# (100 + 120) / 2 = 110 average employees
#
# Now, use the turnover formula:
#
# (15 / 110) x 100 = 13.6%
#
# Your annual employee turnover rate is 13.6%.
#




