import os, logging
import subprocess, threading
from .cmdline_operator import execute_cmd

logger = logging.getLogger(__name__)


def execute(line, **context):
    logger.info(f"Executing sh operator {line}")
    logger.debug(f"Executing sh operator with {line} parameters and context: {context}")

    layout_operator_lookup = context.get("__LAYOUT_OPERATOR__")
    if not layout_operator_lookup:
        logger.error("Layout operator not provided in context")
        return -1

    dataflow = context.get("__DATAFLOW__")
    container = context.get("__CONTAINER__")

    # Get the shell script name
    script_name = layout_operator_lookup(
        dataflow, container, line.get("__PARAM0__"), **context
    )

    if not script_name:
        logger.error("No script name provided in __PARAM0__")
        return -1

    # Collect the script parameters
    script_params = []
    i = 1
    while f"__PARAM{i}__" in line:
        script_params.append(line[f"__PARAM{i}__"])
        i += 1

    # Construct the command
    command = [script_name] + script_params
    return execute_cmd(command)
