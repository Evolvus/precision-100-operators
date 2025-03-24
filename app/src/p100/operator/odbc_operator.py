import logging
from .cmdline_operator import execute_cmd, execute_internal

logger = logging.getLogger(__name__)

def validate(line, **context):
    if not line.get("__PARAM0__"):
        logger.error("No script name provided in __PARAM0__")
        return -1
    
    if not line.get("__PARAM1__"):
        logger.error("No connection name provided in __PARAM1__")
        return -1
    
    return 0

def execute(line, **context):
    status,layout_operator_lookup, dataflow, container, connection_parts, env_vars, default_values = execute_internal(line, "ODBC_SQL", **context)
    if status != 0:
        return status

    status = validate(line, **context)
    if status != 0:
        return status

    # Get the sql script name
    script_name = layout_operator_lookup(
        dataflow, container, line.get("__PARAM0__"), **context
    )
    if not script_name:
        logger.error("Unable to resolve script name")
        return -1

    dsn_name = connection_parts.get('__PARAM3__')
    username = connection_parts.get('__PARAM4__')
    password = connection_parts.get('__PARAM5__')

    connection_string = f"-S{dsn_name} -U {username} -P {password}"
    logger.info(f"Connection string: {connection_string}")

    # Construct the command
    command = [
        "isql",
        connection_string,
        "-i",
        script_name,
    ]
    return execute_cmd(command)