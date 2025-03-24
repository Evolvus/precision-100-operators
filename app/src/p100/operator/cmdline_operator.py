import subprocess, threading
import logging, os
import importlib.resources as pgk_resources
import tomllib as toml

logger = logging.getLogger(__name__)

 # Execute the command
def execute_cmd(command):
    logger.info(f"Executing command: {command}")
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def output_info(stream):
            for line in stream:
                logger.info(line.strip())

        def output_error(stream):
            for line in stream:
                logger.error(line.strip())

        stdout_thread = threading.Thread(target=output_info, args=(process.stdout,))
        stderr_thread = threading.Thread(target=output_error, args=(process.stderr,))

        stdout_thread.start()
        stderr_thread.start()

        process.wait()

        stdout_thread.join()
        stderr_thread.join()

        return process.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing script: {e}")
        return -1

    return 0

def resolve_delimiter(in_delimiter):
    logger.info(f"Resolving delimiter: {in_delimiter}")
    delimiter = in_delimiter.upper()
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

    logger.info(f"Resolved delimiter: {delimiter}")
    return delimiter

def load_config():
    try:
        with pgk_resources.open_binary("p100.operator.pg.resources", "config.toml") as f:
            return toml.load(f)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return None

def execute_internal(line, operator_name, **context):
    logger.info(f"Executing {operator_name} operator {line}")
    logger.debug(
        f"Executing {operator_name} operator with {line} parameters and context: {context}"
    )

    layout_operator_lookup = context.get("__LAYOUT_OPERATOR__")
    if not layout_operator_lookup:
        logger.error("Layout operator not provided in context")
        return -1, None, None, None, None, None, None
    
    dataflow = context.get("__DATAFLOW__")
    container = context.get("__CONTAINER__")

    env_vars = {}
    default_values = {}
    config = load_config()
    default_operator_conf = config.get(operator_name)
    if default_operator_conf:
        env_vars = default_operator_conf.get("ENV", {})
        default_values = default_operator_conf.get("DEFAULT", {})

    # Load custom operator configuration, if present
    operator_conf = context.get("__OPERATOR_CONF__")
    if operator_conf:
        custom_env_vars = operator_conf.get(operator_name, env_vars).get("ENV")
        custom_default_values = operator_conf.get(operator_name, config).get("DEFAULT")
        logger.debug(f"Using ENV from operator configuration: {env_vars}")   
        env_vars.update(custom_env_vars)
        default_values.update(custom_default_values)

    default_connection = default_values.get("CONNECTION", "PRECISION100")
    connection_name = line.get("__PARAM1__", default_connection)

    # Get the connection details
    connect_operator_lookup = context.get("__CONNECT_OPERATOR__")
    if not connect_operator_lookup:
        logger.error("Connect operator not provided in context")
        return -3, None, None, None, None, None, None

    connection_parts = connect_operator_lookup(
        connection_name, **context
    )

    return 0, layout_operator_lookup, dataflow, container, connection_parts, env_vars, default_values