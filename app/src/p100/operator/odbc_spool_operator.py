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

    if not line.get("__PARAM2__"):
        logger.error("No spool file provided in __PARAM2__")
        return -1
    
    return 0

def execute(project_config, execution_config, line, **context):
    status,layout_operator_lookup, dataflow, container, connection_parts, env_vars, default_values = execute_internal(project_config, execution_config, line, "ODBC_SQL", **context)
    if status != 0:
        return status

    status = validate(line, **context)
    if status != 0:
        return status

    # Get the sql 
    sql = line.get("__PARAM0__")

    dsn_name = connection_parts.get('__PARAM3__') # DSN -  IP/PORT
    database = connection_parts.get('__PARAM4__')
    username = connection_parts.get('__PARAM5__')
    password = connection_parts.get('__PARAM6__')

    delimiter = default_values.get("DELIMITER", "COMMA")
    header = default_values.get("HEADER", "YES")
    quote = default_values.get("QUOTE", "YES")

    spool_file = layout_operator_lookup(project_config, execution_config, dataflow, container, line.get("__PARAM2__"), **context)
    if not spool_file:
        logger.error(f"Failed to resolve spool file {line.get('__PARAM2__')}")
        return -1

    delimiter = line.get("__PARAM3__", delimiter)
    delimiter = resolve_delimiter(delimiter)
    header = line.get("__PARAM4__", header)
    quote = line.get("__PARAM5__", quote)

    # Construct the command
    command = [
        "isql",
        dsn_name,
        username,
        password,
        "-b",
        f"-d{delimiter}",
    ]
    if header == "YES":
        command.append("-c")
    
    if quote == "YES":
        command.append("-q")

    return execute_cmd(command=command, input=sql, output=spool_file)