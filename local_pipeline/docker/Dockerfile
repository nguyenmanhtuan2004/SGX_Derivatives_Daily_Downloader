FROM apache/airflow:2.9.2-python3.11

USER root

# Cài đặt OpenJDK 17 JRE cho PySpark JVM
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cấu hình biến môi trường Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Cài đặt các thư viện Python (boto3, requests, PySpark, Delta, v.v.)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
