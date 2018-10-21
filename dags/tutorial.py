# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

'''
    This is a configuration file known as DAG file
'''

############# DEFAULT ARGUMENTS ##################

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 20),
    'email': ['oriolbx@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

############# INSTANTIATE A DAG ##################

# is a collection of all the tasks you want to run, organized in a way that reflects 
# their relationships and dependencies. Is a description of the order in which work should take place

dag = DAG(
    'tutorial', default_args=default_args, schedule_interval=timedelta(1))

############# TASKS AND OPERATORS ##################

# t1, t2 and t3 are examples of tasks created by instantianting operators
# task_id acts as a unique identifier for the task
# A task must include or inherit the arguments task_id and owner, otherwise Airflow will raise an exception.


# Once an operator is instantiated, it is referred to as a “task”. 
# The instantiation defines specific values when calling the abstract operator, 
# and the parameterized task becomes a node in a DAG.

# Task instances also have an indicative state, which could be 
# “running”, “success”, “failed”, “skipped”, “up for retry”, etc.


# Operators do not have to be assigned to DAGs immediately (previously dag was a required argument). 
# However, once an operator is assigned to a DAG, it can not be transferred or unassigned.

# * BashOperator - executes a bash command
# * PythonOperator - calls an arbitrary Python function
# * EmailOperator - sends an email
# * SimpleHttpOperator - sends an HTTP request
# * MySqlOperator, SqliteOperator, PostgresOperator, MsSqlOperator, OracleOperator, JdbcOperator, etc. - executes a SQL command
# * Sensor - waits for a certain time, file, database row, S3 key, etc…

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag
)

# JINJA template
templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)


############# DEPENDENCIES BETWEEN TASKS ##################

# Traditionally, operator relationships are set with the set_upstream() and set_downstream() methods.
# This can be done with the Python bitshift operators >> and <<.


# t2 will depend on t1 running successfully to run
t2.set_upstream(t1) # same as t1 >> t2, t1 runs first and t2 runs second

# t3 will depend on t1 running successfully to run
t3.set_upstream(t1) # same as t1 >> t3, t1 runs first and t3 runs second