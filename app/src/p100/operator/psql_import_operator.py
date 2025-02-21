import logging
from .cmdline_operator import execute_cmd

CONNECTION_TYPE = "PSQL"

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

    table_name = line.get("__PARAM0__")
    if not table_name:
        logger.error("No table name provided in __PARAM0__")
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
    connection_string = f"host={connection_parts.get('__PARAM3__')} port={connection_parts.get('__PARAM4__')} dbname={connection_parts.get('__PARAM5__')} user={connection_parts.get('__PARAM6__')} password={connection_parts.get('__PARAM7__')}"
    logger.info(f"Connection string: {connection_string}")

    # Get the sql spool file name
    spool_file_name = layout_operator_lookup(
        dataflow, container, line.get("__PARAM2__"), **context
    )

    delimiter = ','
    header = 'YES'

    if line.get("__PARAM3__"):
        delimiter = line.get("__PARAM3__").upper()
        if delimiter == 'TAB':
            delimiter = '\t'
        if delimiter == 'COMMA':
            delimiter = ','
        if delimiter == 'PIPE':
            delimiter = '|'
        if delimiter == 'CARAT':
            delimiter = '^'
        if delimiter == 'SPACE':
            delimiter = ' '
        if delimiter == 'SEMICOLON':
            delimiter = ';'
        if delimiter == 'COLON':
            delimiter = ':'
        if delimiter == 'DOLLAR':
            delimiter = '$'
        if delimiter == 'HASH':
            delimiter = '#'
        if delimiter == 'AT':
            delimiter = '@'
        if delimiter == 'AMPERSAND':
            delimiter = '&'
        if delimiter == 'ASTERISK':
            delimiter = '*'
        if delimiter == 'PERCENT':
            delimiter = '%'
        if delimiter == 'EXCLAMATION':
            delimiter = '!'
        if delimiter == 'QUESTION':
            delimiter = '?'
        if delimiter == 'TILDE':
            delimiter = '~'
        if delimiter == 'BACKSLASH':
            delimiter = '\\'
        if delimiter == 'FORWARDSLASH':
            delimiter = '/'
        if delimiter == 'HYPHEN':
            delimiter = '-'
        if delimiter == 'UNDERSCORE':
            delimiter = '_'
        if delimiter == 'PLUS':
            delimiter = '+'

    script_name = f"\\copy {table_name} from '{spool_file_name}' DELIMITER {delimiter}"

    if line.get("__PARAM4__").upper() == 'YES':
       script_name = f"\\copy {table_name} from '{spool_file_name}' DELIMITER '{delimiter}' csv header"


    # Construct the command
    command = [
        "psql",
        connection_string,
        "-c",
        script_name,
    ]
    return execute_cmd(command)