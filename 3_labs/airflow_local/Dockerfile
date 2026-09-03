# Airflow 3.3.1 + a JRE + PySpark, so a BashOperator inside the container can
# run the iceberg_deep lab scripts exactly as EMR would run them.
#
# On MWAA you cannot build an image: dependencies go in requirements.txt and
# system packages are not available at all. That difference is the whole
# reason DAG 04 has a LAB_MODE=emr branch that ships the work to EMR instead.
FROM apache/airflow:3.3.1-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
# Provider under the official constraints for this Airflow build and the
# image's own Python version; then PySpark, which the constraints do not pin.
RUN PYVER=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') \
 && pip install --no-cache-dir "apache-airflow-providers-amazon" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-${PYVER}.txt" \
 && pip install --no-cache-dir "pyspark==4.1.3"
