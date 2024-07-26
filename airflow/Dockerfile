FROM apache/airflow:2.8.4-python3.10
#ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
#ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=my_conn_string
#USER root
#RUN apt-get update \
##    && sudo apt update && sudo apt upgrade && sudo apt-get install python3.9 \
#   && apt-get install -y --no-install-recommends \
#           vim
#ADD requirements.txt .
USER root
RUN apt update
RUN apt install git -y
RUN apt update -y && sudo apt install -y build-essential libpq-dev
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         heimdal-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
#RUN apt-get install vim -y
USER airflow
#RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements3.txt
#RUN pip uninstall -y argparse
#ENV AIRFLOW_VERSION=2.8.4
#ENV PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
ENV wsl_ip="$(cat /etc/resolv.conf | grep nameserver | cut -d ' ' -f 2)"
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install "apache-airflow[async,postgres,google,cncf.kubernetes,snowflake,airbyte,dbt-cloud]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
#RUN pip install "apache-airflow[async,postgres,google,cncf.kubernetes,snowflake,airbyte]==${AIRFLOW_VERSION}" -r requirements2.txt
RUN pip uninstall -y argparse
#RUN apache-airflow-providers-dbt-cloud

RUN pip install great_expectations 
RUN pip install airflow-provider-great-expectations
RUN pip install astronomer-cosmos[dbt.all]

USER root
RUN mkdir -p -m 777 /opt/airflow/ge
RUN mkdir -p -m 777 /opt/airflow/logs
RUN mkdir -p -m 777 /opt/airflow/dags
RUN mkdir -p -m 777 /opt/airflow/plugins
RUN mkdir -p -m 777 /opt/airflow/logs/scheduler
RUN chmod a+x /usr/bin/git