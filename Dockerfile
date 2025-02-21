FROM apache/airflow:2.10.5-python3.12

USER root

RUN  apt-get update --fix-missing && apt-get install -y gcc python3-dev default-jdk && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-jdk/
RUN export JAVA_HOME

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
