import os, logging
import subprocess, threading
from .operator_utils import execute_cmd

logger = logging.getLogger(__name__)

def validate_line(line, **context):
    if "__PARAM0__" not in line:
        logger.error("No script name provided in __PARAM0__")
        return -1
    return 0

def execute(project_config, execution_config, line, **context):
    logger.info(f"Executing {__name__} operator {line}")
    logger.debug(f"Executing {__name__} operator with {line} parameters and context: {context}")

    layout_operator_lookup = context.get("__LAYOUT_OPERATOR__")
    dataflow = context.get("__DATAFLOW__")
    container = context.get("__CONTAINER__")

    status = validate_line(line, **context)
    if status != 0:
        return status

    # Get the shell script name
    script_name = layout_operator_lookup(
        project_config, execution_config, dataflow, container, line.get("__PARAM0__"), **context
    )

    # Collect the script parameters
    script_params = []
    i = 1
    while f"__PARAM{i}__" in line:
        script_params.append(line[f"__PARAM{i}__"])
        i += 1

    # Construct the command
    command = [script_name] + script_params
    return execute_cmd(command)
