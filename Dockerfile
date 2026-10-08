FROM apache/airflow:slim-3.3.0-python3.12

COPY requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir -r /tmp/project-requirements.txt
