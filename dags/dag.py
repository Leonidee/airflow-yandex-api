from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from os import getenv
from pathlib import Path

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

from utils import get_logger
from conns import connect_to_database
from tasks import (
    generate_report_request,
    get_api_response,
    get_report,
    insert_new_data_to_database,
    update_dimension_tables,
    update_fact_tables,
)

find_dotenv(raise_error_if_not_found=True)
load_dotenv(verbose=True, override=True)

logger = get_logger(logger_name=str(Path(Path(__file__).name)))

HOST = getenv("MAIN_DB_HOST")
PORT = getenv("MAIN_DB_PORT")
USER = getenv("MAIN_DB_USER")
PASSWORD = getenv("MAIN_DB_PASSWORD")
DATABASE = "main"
ENDPOINT = getenv("ENDPOINT")
HEADERS = {
    "X-API-KEY": getenv("APIKEY"),
    "X-Nickname": getenv("NICKNAME"),
    "X-Cohort": getenv("COHORT"),
    "X-Project": "True",
}

REPORT_DATE = datetime.today() - timedelta(days=1)

UPDATE_DIM_SQL = "sql/update-dimension-tables.sql"
UPDATE_FACT_SQL = "sql/update-fact-tables.sql"

engine = connect_to_database(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE
)


@dag(
    dag_id="main-dag",
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 2, 20),
    default_args={
        "owner": "leonide",
        "retries": 0,
        "retry_delay": timedelta(seconds=2),
    },
    catchup=False,
)
def taskflow():

    start = EmptyOperator(task_id="start")

    request = generate_report_request(headers=HEADERS, endpoint=ENDPOINT)
    response = get_api_response(request=request, endpoint=ENDPOINT, headers=HEADERS)
    report_response = get_report(
        api_response=response,
        endpoint=ENDPOINT,
        headers=HEADERS,
        report_date=REPORT_DATE,
    )
    customer_research = insert_new_data_to_database(
        engine=engine,
        schema="stage",
        file_name="customer_research",
        api_response=report_response,
    )
    user_order_log = insert_new_data_to_database(
        engine=engine,
        schema="stage",
        file_name="user_order_log",
        api_response=report_response,
        add_status_column=True,
    )
    user_activity_log = insert_new_data_to_database(
        engine=engine,
        schema="stage",
        file_name="user_activity_log",
        api_response=report_response,
    )
    update_dim = update_dimension_tables(
        engine=engine, stage_schema="stage", mart_schema="mart", sql_file=UPDATE_DIM_SQL
    )

    update_fact = update_fact_tables(
        engine=engine,
        stage_schema="stage",
        mart_schema="mart",
        sql_file=UPDATE_FACT_SQL,
        report_date=REPORT_DATE,
    )

    end = EmptyOperator(task_id="end")

    chain(
        start,
        request,
        response,
        report_response,
        [customer_research, user_order_log, user_activity_log],
        update_dim,
        update_fact,
        end,
    )


dag = taskflow()
