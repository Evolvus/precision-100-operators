import os, logging
import csv

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
    with open(connect_store_file, "r") as f:
        reader = csv.reader(f, { lambda line: line.startswith("#") ,  lambda line: line.isspace() })
        for row in reader:
            if len(row) <= 2:
                logger.error(f"Invalid row in connection store file: {row}")
                continue
            logger.debug(f"Row in connection store file: {row}")
            if row[0].upper() == connection_name.upper():
                connection = {f"__PARAM{i+1}__": value for i, value in enumerate(row)}
                break
    logger.debug(f"Connection: {connection}")
    return connection
