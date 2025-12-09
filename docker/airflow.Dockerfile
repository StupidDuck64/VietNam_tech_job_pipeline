# ===== Dockerfile cho Airflow =====
# Image này sẽ chạy cả Airflow Webserver lẫn Scheduler

FROM apache/airflow:2.7.3-python3.11

USER root

    # ===== Cài đặt system dependencies + Chrome =====
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    openjdk-11-jdk \
    wget \
    gnupg \
    unzip \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# ===== Download và cài ChromeDriver riêng =====
RUN FULL_CHROME_VERSION=$(google-chrome --version | awk '{print $3}') \
    && CHROME_MAJOR=$(echo $FULL_CHROME_VERSION | cut -d '.' -f 1) \
    && echo "Chrome version: $FULL_CHROME_VERSION, Major: $CHROME_MAJOR" \
    && wget -q "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_MAJOR}" -O /tmp/latest_version \
    && CHROMEDRIVER_VERSION=$(cat /tmp/latest_version) \
    && echo "ChromeDriver version: $CHROMEDRIVER_VERSION" \
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CHROMEDRIVER_VERSION}/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip \
    && unzip -q /tmp/chromedriver.zip -d /tmp/ \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/chromedriver* /tmp/latest_version \
    && chromedriver --version# ===== Java env for PySpark =====
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
