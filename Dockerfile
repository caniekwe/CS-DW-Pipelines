FROM apache/airflow:3.1.7-python3.10
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt