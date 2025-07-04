# Project Overview
## Data Carpentry - Assignment 2
#### by Jack Toke

# Introduction

This project is a data pipeline for transforming and loading data as a part of Data Carpentry (IFQ718).
The object of the project is to find answers to three specific questions.
1. Are there periods of the year, where some businesses are more profitable?
2. Which customers were most loyal for each business?
3. What is the employee turnover rate of each business?

# Pipeline Architecture
![Tech stack](./images/tools_banner.png)
The project integrates **Apache Airflow**, **dbt**, **Streamlit** and **DuckDB** to run on **Docker**.
It is meant to be portable and doesn't need any other services.

# How to get up and running?
1. First you will need Docker installed on your system.
2. Clone the repo
3. Once you have cd into the directory where you have persisted the repository, setup the **Python** virtual environment.
`python -m .venv 3.12`.  Then you will have activate your **Python** virtual environment by running `source .venv/bin/activate`.
4. You will also need to install the required dependencies by running `pip install -r requirements.txt`.
5. Finally, you can run `docker compose up`.  You can visit `localhost:8080` to access the **Airflow** dashboard, 
where you can run all your dags that transform and load the **JSON** files provided.  The files are located in `./data/receipts/*`.
6. To see the visual you can visit `localhost:8505`.

# Airflow
Once, your docker is up and running, visit `localhost:8080` on your browser and you will be greeted by the following login screen.
The default username and password is `airflow` for both the username and password.

![Login Page](./images/airflow_login.jpg)

Once you are logged in your will see the following screen.
![Dashboard](./images/airflow_dashboard1.jpg)
Click on the **Dags** icon on the left of the screen and you can 

### ❗Warning❗

This template used DuckDB, an in-memory database, for running dbt transformations. While this is great to learn Airflow, your data is not guaranteed to persist between executions! For production applications, use a _persistent database_ instead (consider DuckDB's hosted option MotherDuck or another database like Postgres, MySQL, or Snowflake).
