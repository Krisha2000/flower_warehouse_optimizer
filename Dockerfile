# Start FROM the official, production-ready Airflow image
FROM apache/airflow:2.9.2

# Install the Python libraries needed for our project
RUN pip install --no-cache-dir pandas streamlit psycopg2-binary

# Copy ALL project folders into the image's filesystem
COPY ./scripts /opt/airflow/scripts
COPY ./data /opt/airflow/data
COPY ./dags /opt/airflow/dags