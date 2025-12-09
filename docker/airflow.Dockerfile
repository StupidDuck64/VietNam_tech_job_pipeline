# ===== Dockerfile cho Airflow =====
# Image này sẽ chạy cả Airflow Webserver lẫn Scheduler

FROM apache/airflow:2.7.3-python3.11

USER root

# ===== Cài đặt system dependencies =====
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# ===== Java env for PySpark =====
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# ===== Chuyển về airflow user =====
USER airflow

# ===== Copy requirements.txt và cài đặt Python packages =====
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ===== Set AIRFLOW_HOME =====
ENV AIRFLOW_HOME=/opt/airflow

# ===== Health check =====
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["airflow", "webserver"]
