from funcs import (
    create_schema_and_tables,
    generate_data_request,
    get_api_response,
    insert_data_to_database,
    update_datamarts,
)

from utils import get_logger
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from os import getenv
from conns import connect_to_database

find_dotenv(raise_error_if_not_found=True)
load_dotenv(verbose=True, override=True)

INIT_SQL = "sql/init.sql"
UPDATE_DATAMARTS_SQL = "sql/update-datamarts.sql"

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
}

engine = connect_to_database(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE
)


def main():

    logger.info("Starting initializing process.")

    create_schema_and_tables(engine=engine, sql_file=INIT_SQL)

    request = generate_data_request(endpoint=ENDPOINT, headers=HEADERS)
    response = get_api_response(request=request, endpoint=ENDPOINT, headers=HEADERS)

    insert_data_to_database(
        response=response,
        engine=engine,
        schema="stage",
        target_table="customer_research",
    )

    insert_data_to_database(
        response=response,
        engine=engine,
        schema="stage",
        target_table="user_activity_log",
    )

    insert_data_to_database(
        response=response,
        engine=engine,
        schema="stage",
        target_table="user_order_log",
        add_status_column=True,
    )

    update_datamarts(engine=engine, sql_file=UPDATE_DATAMARTS_SQL)

    logger.info("Intializing process complited successfully.")


if __name__ == "__main__":
    main()
