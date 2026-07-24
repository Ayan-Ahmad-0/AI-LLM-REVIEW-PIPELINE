FROM apache/airflow:2.5.1-python3.9

USER root

# System dependencies (curl needed for the webserver healthcheck in docker-compose)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-dev \
        wget \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Make sure 'python' points to 'python3'
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Switch to airflow user before any pip install (Airflow images refuse root pip installs)
USER airflow

# Install Python packages from a pinned requirements file instead of inline pip install —
# keeps versions reproducible and makes it a one-line change to add a package later.
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# NOTE: no COPY of ./src or ./scripts here on purpose — docker-compose.yml already
# volume-mounts ./src into /opt/airflow/src, so code changes show up without a rebuild.
# If you later deploy this somewhere without volume mounts (e.g. a real cluster),
# add a COPY step here at that point.