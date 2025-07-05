FROM python:3.12-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY dashboard/requirements.txt .

# Install dependencies as root
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user
# The user's home directory will be /app
RUN useradd -m -d /app -s /bin/bash appuser

# Copy the rest of the application code and set ownership
COPY --chown=appuser:appuser ./dashboard ./dashboard
COPY --chown=appuser:appuser ./dbt/airbnb/receipts.duckdb ./dbt/airbnb/receipts.duckdb

# Switch to the non-root user
USER appuser

# Expose the port Streamlit runs on
EXPOSE 8501

# Command to run the Streamlit app
CMD streamlit run dashboard/streamlit_app.py --server.headless=true --server.port $PORT --server.enableCORS=false --server.enableXsrfProtection=false --server.enableCORS=false --server.enableXsrfProtection=false
