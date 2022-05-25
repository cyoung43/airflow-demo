# pyright: reportMissingImports=false
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# import snowflake.connector as sc
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtSeedOperator, DbtSnapshotOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.version import version
from airflow.decorators import dag
from datetime import datetime, timedelta
import pytz
from airflow import settings
import json

################################# VARIABLES #################################
NBA = Variable.get('airbyte_connection_id_nba')
SLACK_CONN_ID = Variable.get('slack')


#### Helper Functions ####
def convert_datetime(datetime_string):

    return datetime_string.astimezone(pytz.timezone('America/Denver')).strftime('%b-%d %H:%M:%S')


##### Slack Alerts #####
def task_fail_slack_alert(context):
    # Called on failure
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_msg = f"""
        :x: Task Failed.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('execution_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)

def task_succeed_slack_alert(context):
    # Called on success
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_msg = f"""
        :white_check_mark: Task Succeeded!
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('execution_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id='slack_success',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)


# Default args for Airflow DAGs
default_args={
    "owner":"airflow",
    "retries":1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 12, 1),
    "catchup": False,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_succeed_slack_alert
}

################################## DAG ######################################
@dag(
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['airbyte']
)
def airbyte_dag():
    '''
    ### Airbyte DAG
    This is a simple ELT data pipeline dag that demonstrates the AirbyteTriggerSyncOperator, which makes POST requests to 
    Airbyte's API. The operator will kick off an airbyte sync job, after which the DBT operation will run the transformation piece
    of the Airflow dag.

    For more info, check out the README.md
    '''

    # Dummy operator: usually used as a start node
    t0 = DummyOperator(
        task_id='start'
    )

    # If DBT isn't located, run a bash command to install and check that the bin location exists
    # if not, insert this command to run => pip3 install airflow-dbt && pip3 install dbt-snowflake
    # test = BashOperator(
    #     task_id='test',
    #     bash_command='cd /home/astro/.local/bin && ls -a'
    # )

    # DBT Run
    # /home/astro/.local/bin/dbt
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir="/usr/local/airflow/dags/dbt/",
        dbt_bin='/home/astro/.local/bin/dbt',
        profiles_dir='/usr/local/airflow/dags/dbt/',
        trigger_rule="all_done", # Run even if previous tasks failed
    )


    extract_nba = AirbyteTriggerSyncOperator(
        task_id='extract_nba',
        airbyte_conn_id='airbyte',
        connection_id=NBA,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    # t0 >> create_tables >> dbt_seed >> dbt_snapshot >> dbt_run
    t0 >> extract_nba >> dbt_run
    # t0 >> test

airbyte_dag = airbyte_dag()