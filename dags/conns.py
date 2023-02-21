from sqlalchemy.engine import Engine
from sqlalchemy import create_engine

from utils import get_logger
from pathlib import Path
from exceptions import DatabaseConnectionError

logger = get_logger(logger_name=str(Path(Path(__file__).name)))


def connect_to_database(
    user: str, password: str, host: str, port: str, database: str
) -> Engine:

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )

    try:
        conn = engine.connect()
        conn.close()
        logger.info(f"Succesfully connected to `{database}` database.")

    except Exception:
        logger.exception(f"Connection to `{database}` database failed!")
        raise DatabaseConnectionError

    return engine


if __name__ == "__main__":
    connect_to_database()
