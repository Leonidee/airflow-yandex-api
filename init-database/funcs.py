from sqlalchemy.engine import Engine
from pandas import read_csv

from sqlalchemy.engine import Engine
import requests
import time
from pathlib import Path
from utils import get_logger
from exceptions import SQLError, ApiServiceError, S3ServiceError

logger = get_logger(logger_name=str(Path(Path(__file__).name)))


def create_schema_and_tables(engine: Engine, sql_file: str) -> None:

    logger.info("Initializing schemas and tables in main database.")

    query = Path(Path(__file__).parent, sql_file).read_text(encoding="UTF-8")
    try:
        with engine.connect() as conn:
            conn.execute(query)
        logger.info(
            f"{sql_file} file was successfully executed. Schemas and tables was initialized."
        )
    except Exception:
        logger.exception(
            f"Can't execture {sql_file} query. Initializing process failed!"
        )
        raise SQLError


def generate_data_request(endpoint: str, headers: dict) -> dict:

    logger.info("Sending data generation request to API.")
    try:
        request = requests.post(
            f"{endpoint}/generate_report",
            headers=headers,
        ).json()
    except Exception:
        logger.exception("Can't send request to API.")
        raise ApiServiceError

    return request


def get_api_response(request: dict, endpoint: str, headers: dict) -> dict:

    task_id = request["task_id"]
    attempt = 1
    max_attempt_cnt = 8

    for _ in range(max_attempt_cnt):
        try:
            logger.info(f"Trying to get response. Attempt: {attempt}")
            response = requests.get(
                f"{endpoint}/get_report?task_id={task_id}", headers=headers
            ).json()

            if "status" in response.keys():
                if response["status"] == "SUCCESS":
                    logger.info("Response recieved!")
                    break
        except Exception:
            logger.exception("Bad API request. Something went wrong!")
            raise ApiServiceError

        if attempt == max_attempt_cnt:
            logger.exception("API is not responding.")
            raise ApiServiceError

        attempt += 1
        time.sleep(20)

    return response


def insert_data_to_database(
    response: dict,
    engine: Engine,
    schema: str,
    target_table: str,
    add_status_column: bool = False,
) -> None:
    file_name = target_table

    logger.info(f"Loading {file_name} file from s3.")
    try:
        df = read_csv(filepath_or_buffer=response["data"]["s3_path"][f"{file_name}"])

        if "uniq_id" in df.columns:
            df = df.drop_duplicates(subset=["uniq_id"], keep=False)
            df = df.drop(columns="uniq_id")

        if "id" in df.columns:
            df = df.drop(columns="id")

        if add_status_column == True:
            if "status" not in df.columns:
                df["status"] = "shipped"

        logger.info(f"Successfully loaded {file_name} file.")

    except Exception:
        logger.exception(f"Can't load {file_name} from s3.")
        raise S3ServiceError

    logger.info(f"Uploading data to `{schema}.{target_table}` table.")

    try:
        df.to_sql(
            name=file_name,
            if_exists="replace",
            schema=schema,
            con=engine,
            index=False,
        )

        logger.info(f"`{schema}.{target_table}` table was successfully updated.")

    except Exception:
        logger.exception(f"Can't insert date to `{schema}.{target_table}`.")
        raise SQLError


def update_datamarts(engine: Engine, sql_file: str) -> None:
    logger.info(f"Updating datamarts")

    query = Path(Path(__file__).parent, sql_file).read_text(encoding="UTF-8")

    try:
        logger.info(f"Starting execute {sql_file} query.")

        with engine.connect() as conn:
            conn.execute(query)
            logger.info(
                f"{sql_file} file was successfully executed. Datamarts were updated."
            )
    except Exception:
        logger.exception(f"Can't execute {sql_file} file.")
        raise SQLError


def testing() -> None:
    """Testing fucntionality"""

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
        response=response, engine=engine, schema="stage", target_table="user_order_log"
    )

    update_datamarts(engine=engine, sql_file=UPDATE_DATAMARTS_SQL)


if __name__ == "__main__":
    testing()
