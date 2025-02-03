import os, logging
import subprocess, threading

logger = logging.getLogger(__name__)


def execute(line, **context):
    logger.info(f"Executing sh operator {line}")
    logger.debug(f"Executing sh operator with {line} parameters and context: {context}")

    layout_operator = context.get("__LAYOUT_OPERATOR__")
    if not layout_operator:
        logger.error("Layout operator not provided in context")
        return -1

    dataflow = context.get("__DATAFLOW__")
    container = context.get("__CONTAINER__")

    # Get the shell script name
    script_name = layout_operator.lookup(
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
    logger.info(f"Executing command: {command}")

    # Execute the command
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
