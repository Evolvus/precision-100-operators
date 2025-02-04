import os, logging
import csv

PARAM_ENV = "env"

logger = logging.getLogger(__name__)


def lookup(connect_operator_name, connection_name, **context):
    logger.info(f"Lookup connection {connection_name} for {connect_operator_name}")
    logger.debug(
        f"Lookup connection {connection_name} for {connect_operator_name} with context: {context}"
    )

    my_env = context.get(PARAM_ENV)
    if not my_env:
        logger.error("Environment not provided in context")
        return {}

    connect_store_file = my_env.get("PRECISION100_PROJECT_CONNECTION_FILE")
    if not os.path.isfile(connect_store_file):
        logger.error(f"Connection store file does not exist: {connect_store_file}")
        return {}

    connection = {}
    with open(connect_store_file, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) <= 2:
                logger.error(f"Invalid row in connection store file: {row}")
                continue
            logger.info(f"Row in connection store file: {row}")
            if (
                row[1].upper() == connect_operator_name.upper()
                and row[0].upper() == connection_name.upper()
            ):
                connection = {f"__PARAM{i+1}__": value for i, value in enumerate(row)}
                break
    logger.info(f"Connection: {connection}")
    return connection
