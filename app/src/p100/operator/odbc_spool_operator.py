import logging
from .cmdline_operator import execute_cmd, execute_internal, resolve_delimiter

logger = logging.getLogger(__name__)

def validate(line, **context):
    if not line.get("__PARAM0__"):
        logger.error("No sql provided in __PARAM0__")
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

    # Get the sql 
    sql = line.get("__PARAM0__")

    dsn_name = connection_parts.get('__PARAM3__')
    username = connection_parts.get('__PARAM4__')
    password = connection_parts.get('__PARAM5__')

    connection_string = f"{dsn_name} {username} {password}"
    logger.info(f"Connection string: {connection_string}")

    delimiter = default_values.get("DELIMITER", ",")
    header = default_values.get("HEADER", "YES")
    quote = default_values.get("QUOTE", "YES")

    delimiter = line.get("__PARAM2__", delimiter)
    if delimiter:
        delimiter = resolve_delimiter(delimiter)
    header = line.get("__PARAM3__", header)
    quote = line.get("__PARAM4__", quote)

    # Construct the command
    command = [
        "isql",
        connection_string,
        "-b",
        f"-d{delimiter}",
    ]
    if header == "YES":
        command.append("-c")
    
    if quote == "YES":
        command.append("-q")

    return execute_cmd(command=command, input=sql)