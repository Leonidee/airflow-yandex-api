import requests
import time

from pandas import read_csv
from datetime import datetime
from sqlalchemy.engine import Engine
from pathlib import Path

from airflow.decorators import task

from utils import get_logger
from exceptions import SQLError, ApiServiceError, S3ServiceError

logger = get_logger(logger_name=str(Path(Path(__file__).name)))


@task
def generate_report_request(headers: dict, endpoint: str) -> dict:

    logger.info("Sending data generation request to API.")

    try:
        request = requests.post(f"{endpoint}/generate_report", headers=headers).json()
    except Exception:
        logger.info("Can't send request to API.")
        raise ApiServiceError

    return request


@task
def get_api_response(request: dict, endpoint: str, headers: dict) -> dict:

    task_id = request["task_id"]
    attempt = 1
    max_attempt_cnt = 8

    for _ in range(max_attempt_cnt):
        try:
            logger.info(f"Trying to get response. Attempt: {attempt}.")
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
            logger.info("API is not responding.")
            raise ApiServiceError

        attempt += 1
        time.sleep(20)

    return response


@task
def get_report(
    api_response: dict, endpoint: str, headers: dict, report_date: datetime
) -> dict:
    logger.info("Getting report.")

    report_id = api_response["data"]["report_id"]
    dt = report_date.strftime(format="%Y-%m-%d") + "T00:00:00"
    attempt = 1
    max_attempt_cnt = 8

    for _ in range(max_attempt_cnt):
        try:
            logger.info(f"Trying to get response. Attempt: {attempt}.")
            r = requests.get(
                f"{endpoint}/get_increment?report_id={report_id}&date={dt}",
                headers=headers,
            ).json()

            if "status" in r.keys():
                if r["status"] == "SUCCESS":
                    logger.info("Response recieved!")
                    break

                if r["status"] == "NOT FOUND":
                    logger.info(f"Requested data wasn't found.")
                    raise ApiServiceError
        except Exception:
            logger.exception("Bad API request. Something went wrong!")
            raise ApiServiceError

        if attempt == max_attempt_cnt:
            logger.info("API is not responding.")
            raise ApiServiceError

        attempt += 1
        time.sleep(20)

    return r


@task
def insert_new_data_to_database(
    engine: Engine,
    schema: str,
    file_name: str,
    api_response: dict,
    add_status_column: bool = False,
) -> None:
    """Reads data from s3 into memory and then inserts to `stage` layer of `main` database.

    Args:
        engine (Engine): sqlalchemy.engine.Engine class object.
        schema (str): Schema of database.
        file_name (str): Name of file located on s3. Refer to table name.
        api_response (dict): Yandex API response.
        add_status_column (bool, optional): Should or not add a `status` column into table. Defaults to False.

    Raises:
        S3ServiceError
        SQLError
    """
    logger.info(f"Loading {file_name} from s3.")
    table_name = file_name
    try:
        df = read_csv(
            filepath_or_buffer=api_response["data"]["s3_path"][f"{file_name}_inc"]
        )
        if "uniq_id" in df.columns:
            df = df.drop_duplicates(subset=["uniq_id"], keep=False)
            df = df.drop(columns="uniq_id")
        if "id" in df.columns:
            df = df.drop(columns="id")

        if add_status_column == True:
            if "status" not in df.columns:
                df["status"] = "shipped"

    except Exception:
        logger.exception(f"Can't load {file_name} from s3!")
        raise S3ServiceError

    logger.info(f"Successfully downloaded {file_name} file.")
    logger.info(f"Inserting data to `{schema}.{table_name}` table.")
    try:
        with engine.connect() as conn:
            row_num = df.to_sql(
                name=table_name,
                if_exists="append",
                schema=schema,
                con=conn,
                index=False,
            )
        logger.info(
            f"Successfully inserted {file_name} to `{schema}.{table_name}` table. {row_num} rows were inserted."
        )
    except Exception:
        logger.exception(f"Can't insert data to `{schema}.{table_name}` table!")
        raise SQLError


@task
def update_dimension_tables(
    engine: Engine, stage_schema: str, mart_schema: str, sql_file: Path
) -> None:

    logger.info(f"Updating dimension tables in `{mart_schema}` schema.")
    try:
        logger.info(f"Reading {sql_file} file.")
        query = Path(Path(__file__).parent, sql_file).read_text(encoding="UTF-8")
    except Exception:
        logger.exception(f"Unable to read {sql_file} file!")
        raise Exception
    try:
        with engine.connect() as conn:
            conn.execute(
                statement=(
                    query.format(mart_schema=mart_schema, stage_schema=stage_schema)
                )
            )
        logger.info(
            f"Dimension tables in `{mart_schema}` schema were successfully updated."
        )
    except Exception:
        logger.exception(f"Updating dimension tables in `{mart_schema}` failed.")
        raise SQLError


@task
def update_fact_tables(
    engine: Engine,
    stage_schema: str,
    mart_schema,
    sql_file: Path,
    report_date: datetime,
) -> None:

    logger.info(f"Updating fact tables in `{mart_schema}` schema.")

    try:
        logger.info(f"Reading {sql_file} file.")
        query = Path(Path(__file__).parent, sql_file).read_text(encoding="UTF-8")
    except Exception:
        logger.info(f"Unable to read {sql_file} file!")
        raise Exception

    try:
        with engine.connect() as conn:
            conn.execute(
                statement=(
                    query.format(
                        mart_schema=mart_schema,
                        stage_schema=stage_schema,
                        report_date=str(report_date),
                    )
                )
            )
        logger.info(f"Fact tables in `{mart_schema}` schema were successfully updated.")
    except Exception:
        logger.info(f"Updating fact tables in `{mart_schema}` failed.")
        raise SQLError
