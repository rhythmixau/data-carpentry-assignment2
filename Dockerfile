FROM apache/airflow:latest
WORKDIR /opt/airflow/
COPY requirements.txt requirements.txt
COPY dbt/ dbt/
COPY config/airflow.cfg config/airflow.cfg
COPY .venv/ .venv/
COPY dags/ dags/
RUN pip install --no-cache-dir -r requirements.txt