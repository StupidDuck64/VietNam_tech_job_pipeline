# ===== Dockerfile for Spark =====
# This image will run Spark Master and Worker (Customized)

FROM apache/spark:3.5.0-python3

USER root

# ===== Install Python packages for Spark =====
RUN pip install --no-cache-dir \
    pymongo \
    psycopg2-binary \
    pandas \
    python-dotenv \
    beautifulsoup4 \
    lxml

# ===== Create directory for scripts and data =====
RUN mkdir -p /opt/spark/scripts /opt/spark/data
WORKDIR /opt/spark/work-dir

# ===== Set environment =====
ENV SPARK_HOME=/opt/spark

EXPOSE 7077 8080 8081

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
