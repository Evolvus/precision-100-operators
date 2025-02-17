import os, logging
import subprocess, threading
from .cmdline_operator import execute_cmd

CONNECTION_TYPE = "MSSQL"

logger = logging.getLogger(__name__)


def execute(line, **context):
    logger.info(f"Executing {__name__} operator {line}")
    logger.debug(
        f"Executing {__name__} operator with {line} parameters and context: {context}"
    )

    layout_operator_lookup = context.get("__LAYOUT_OPERATOR__")
    if not layout_operator_lookup:
        logger.error("Layout operator not provided in context")
        return -1

    dataflow = context.get("__DATAFLOW__")
    container = context.get("__CONTAINER__")

    # Get the sql script name
    script_name = layout_operator_lookup(
        dataflow, container, line.get("__PARAM0__"), **context
    )

    if not script_name:
        logger.error("No script name provided in __PARAM0__")
        return -1

    connection_name = line.get("__PARAM1__")
    if not connection_name:
        logger.error("No connection name provided in __PARAM1__")
        return -1

    # Get the connection details
    connect_operator_lookup = context.get("__CONNECT_OPERATOR__")
    if not connect_operator_lookup:
        logger.error("Connect operator not provided in context")
        return -1

    connection_parts = connect_operator_lookup(
        CONNECTION_TYPE, connection_name, **context
    )

    server = connection_parts.get('__PARAM3__')
    port = connection_parts.get('__PARAM4__')
    database = connection_parts.get('__PARAM5__')
    username = connection_parts.get('__PARAM6__')
    password = connection_parts.get('__PARAM7__')
    additional_params = connection_parts.get('__PARAM8__')

    if port:
        connection_string = f"-S tcp:{server},{port} -d {database} -U {username} -P {password}"
    else:
        connection_string = f"-S tcp:{server} -d {database} -U {username} -P {password}"

    if additional_params:
        connection_string += f" {additional_params}"                                                     
    logger.info(f"Connection string: {connection_string}")

    # Construct the command
    command = [
        "sqlcmd",
        connection_string,
        "-i",
        script_name,
    ]
    return execute_cmd(command)