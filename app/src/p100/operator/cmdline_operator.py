import logging
import importlib.resources as pgk_resources
import tomllib as toml
from . import operator_utils

logger = logging.getLogger(__name__)

def load_config():
    try:
        with pgk_resources.open_binary(
            "p100.operator.resources", "config.toml"
        ) as f:
            return toml.load(f)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return None

def execute_internal(project_config, execution_config, line, operator_name, **context):
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
        project_config, execution_config, connection_name, **context
    )

    return (
        0,
        layout_operator_lookup,
        dataflow,
        container,
        connection_parts,
        env_vars,
        default_values,
    )

def get_default_value(property_name, operator_name, **context):
    # load the local configuration
    config = load_config()
    return operator_utils.get_default_value(config, operator_name, property_name, **context)