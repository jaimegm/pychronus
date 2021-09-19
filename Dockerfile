# -- base --
FROM apache/airflow:2.0.2-python3.8

# Enables Python tracebacks on segfaults
# and disable pyc files from being written at import time
ENV PYTHONFAULTHANDLER=1 \
    PYTHONBUFFERED=1  \
    PYTHONDONTWRITEBYTECODE=1

# --------------------------- #
#      AIRFLOW CONFIG         #
# All values can be overriden #
# in the Helm chart or K8s    #
# deployment definitions      #
# --------------------------- #

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__PARALLELISM=8
ENV AIRFLOW__CORE__DAG_CONCURRENCY=16
ENV AIRFLOW__WEBSERVER__BASE_URL="http://localhost:8080"

# Email related
ENV AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp
ENV AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
ENV AIRFLOW__SMTP__SMTP_STARTTLS=True
ENV AIRFLOW__SMTP__SMTP_SSL=False
# ENV AIRFLOW__SMTP__SMTP_USER=
# ENV AIRFLOW__SMTP__SMTP_PASSWORD=
ENV AIRFLOW__SMTP__SMTP_PORT=587
# ENV AIRFLOW__SMTP__SMTP_MAILFROM=

# Logging
ENV AIRFLOW__LOGGING__REMOTE_LOGGING=False
# ENV AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=
# ENV AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=
# ENV AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=
# ENV AIRFLOW__LOGGING__LOGGING_LEVEL = INFO

# Other useful configurations
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
ENV AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
ENV AIRFLOW__CORE__SECURE_MODE=True
# Increase processor timeout to avoid timeout when processing a large dags.py
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=240
ENV AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=240

ARG PYTHON_DEPS

ARG DAGS_PATH

COPY $PYTHON_DEPS .

ADD pychronus $AIRFLOW_HOME/dags/pychronus

# Fix upstream container file mode bits on passwd and GID 0 for airflow user
# The RUN command belong require to be run as root
USER root
RUN chmod 0644 /etc/passwd && usermod -g airflow airflow

# revert container user to airflow (UID 5000)
USER airflow

RUN pip install -r $PYTHON_DEPS
