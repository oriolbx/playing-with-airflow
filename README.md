## Steps initial setup

* export AIRFLOW_HOME="$(pwd)"
* Create dags folder and store dag files there

## Run Airflow

* export AIRFLOW_HOME="$(pwd)"
* airflow initdb
* airflow scheduler
* Open a different terminal tab
* export AIRFLOW_HOME="$(pwd)"
* airflow webserver --port 8080
