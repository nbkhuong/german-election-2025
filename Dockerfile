FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
