import os, logging
import csv

from p100.operator.operator_utils import get_list_from_csv

logger = logging.getLogger(__name__)


def lookup(project_config, execution_config, connection_name, **context):
    logger.debug(f"Lookup connection {connection_name}")
    logger.debug(
        f"Lookup connection {connection_name} with context: {context} project_config: {project_config} execution_config: {execution_config}"
    )

    connect_store_file = project_config.get("PRECISION100_PROJECT_CONNECTION_FILE")
    if not os.path.isfile(connect_store_file):
        logger.error(f"Connection store file does not exist: {connect_store_file}")
        return {}

    connection = {}
    connection_list = get_list_from_csv(connect_store_file)
    if len(connection_list) == 0:
        logger.error(f"Connection store file is empty: {connect_store_file}")
        return connection

    for row in connection_list:
        if len(row) <= 2:
            logger.error(f"Invalid row in connection store file: {row}")
            continue
        logger.debug(f"Row in connection store file: {row}")
        if row[0].upper() == connection_name.upper():
            connection = {f"__PARAM{i+1}__": value for i, value in enumerate(row)}
            break
    logger.debug(f"Connection: {connection}")
    return connection
