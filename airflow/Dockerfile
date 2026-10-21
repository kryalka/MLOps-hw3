FROM apache/airflow:2.10.5-python3.11
USER airflow
COPY requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/project-requirements.txt
RUN pip install --no-cache-dir "cloudpickle==2.2.1" "joblib==1.4.2" && pip check
COPY --chown=airflow:root pipeline /opt/airflow/pipeline
COPY --chown=airflow:root fraud_detector /opt/airflow/fraud_detector
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root airflow/start.sh /opt/airflow/start.sh
