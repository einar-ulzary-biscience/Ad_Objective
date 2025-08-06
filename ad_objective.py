import pandas
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import os

import repositories.exasol as Exasol
import repositories.aws as AWS
import repositories.snowflake as Snowflake
import utils.email as Email
import utils.sms as Sms
import utils.jira as Jira
import sys
import config._config as config
from datetime import datetime
import uuid
import csv
import json


# Multithreading Options:
import time
import concurrent.futures
from threading import Thread, current_thread
from multiprocessing import Process, current_process
import queue
import random
import threading

# pip install apache-airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.models.xcom import XCom
from airflow.operators.email_operator import EmailOperator



# Setup the loggers:
# Note: clear the logging handlers before setting the format:
logging.root.handlers = []
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] | %(name)s | %(filename)s:%(lineno)d | %(message)s",
                    handlers=[
                        logging.StreamHandler(),
                        # RotatingFileHandler(filename='app.log', maxBytes=100000),
                        TimedRotatingFileHandler(
                            'app.log', when='D', interval=1, backupCount=5)
                    ])
logger = logging.getLogger(__name__)

BASE_FOLDER = Path(__file__).parent
INPUTS_FOLDER = Path(BASE_FOLDER, "inputs")
OUTPUTS_FOLDER = Path(BASE_FOLDER, "outputs")
QUERIES_FOLDER = Path(BASE_FOLDER, "queries")

DATE_STRING = str(datetime.now().strftime('%Y%m%d'))
DATETIME_STRING = str(datetime.now().strftime('%Y%m%d_%H%M'))


BULKSIZE = 1000
CAMPAIGNS_TABLE = 'prod_test.CAMPAIGNS_einar'


def getBulk():
    """
    Note: the returned value will be push automatically as xcom 'return_value' and can be read automatically with task_instance.xcom_pull(task_ids='<taskid>')
    """
    logger.info("Getting # of input records from Exasol...")
    try:
        # Note: connect to PROD to get real data:
        exasol = Exasol.connection()
        df = exasol.exportToPandas(query=
            f"""
            SELECT id, lp_url
            FROM (
                SELECT id, lp_url, ROW_NUMBER() OVER () AS rn
                FROM {CAMPAIGNS_TABLE}
                WHERE ad_objective_id IS NULL
            ) t
            WHERE rn <= {int(BULKSIZE * 0.9)}

            UNION ALL

            SELECT id, lp_url
            FROM (
                SELECT id, lp_url, ROW_NUMBER() OVER () AS rn
                FROM {CAMPAIGNS_TABLE}
                WHERE ad_objective_id = -1
            ) t
            WHERE rn <= {int(BULKSIZE * 0.9)};
        """)
        print(df.head(10))
    except Exception as e:
        raise Exception("Failed to get the input records: " + str(e))
    return df


def callobjectiveMethod():
    """
    Getting the bulk from xcom and run in objectiveMethod
    """
    try:
        logger.info("calling objectiveMethod")
        context = get_current_context()
        task_instance = context["ti"]
        dfbulk = task_instance.xcom_pull(task_ids='getBulk')
        df =  objectiveMethod(dfbulk)
    
    except Exception as e:
        raise Exception(
            "Failed to call objectiveMethod" + str(e))
    return df

# Notify users
def mergeintocampaigns(title=None, body=None):
    """
    Getting the prepared bulk from xcom and merge into campaigns table
    """
    try:
        logger.info("Getting df from the method")
        context = get_current_context()
        task_instance = context["ti"]
        dfbulktomerge = task_instance.xcom_pull(task_ids='objectiveMethod')

        logger.info("Merge into campaigns table")

        # Note: connect to PROD to merge data:
        exasol = Exasol.connection()
        staging_table = "prod_test.temp_bulk_merge"
        exasol.importFromPandas(dfbulktomerge, staging_table)

        logger.info(f"Uploaded {len(dfbulktomerge)} rows to {staging_table}")
        query= f"""
            MERGE INTO {CAMPAIGNS_TABLE} TRG
            USING {staging_table} SRC
            ON TRG.id = SRC.campaign_id
            WHEN MATCHED THEN UPDATE SET
            TRG.ad_objective_id = SRC.ad_objective_id
            """
        exasol.execute(query)
        logger.info("Merge completed successfully")
        
    except Exception as e:
        raise Exception(
            "Failed to merge into campaigns table" + str(e))



with DAG(dag_id="ad_objective-dag",
        start_date=datetime(2025,1,1),
        schedule_interval="0 10 * * *",
        catchup=False) as dag:
        
        prepareBulk = PythonOperator(
                task_id="getBulk",
                python_callable=getBulk)
        
        callObjectiveMethod = PythonOperator(
                task_id="callobjectiveMethod",
                python_callable=callobjectiveMethod)

        mergeBack = PythonOperator(
                task_id="mergeintocampaigns",
                python_callable=mergeintocampaigns)


prepareBulk >> callObjectiveMethod >> mergeBack

