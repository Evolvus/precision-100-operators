import os, logging
import subprocess, threading

OPERATOR_NAME = "duckdb_operator"
CONNECTION_TYPE = "DUCKDB"

logger = logging.getLogger(__name__)


def execute(project_config, execution_config, line, **context):
    logger.info(f"Executing {OPERATOR_NAME} operator {line}")
    logger.debug(
        f"Executing {OPERATOR_NAME} with {line} parameters and context: {context}"
    )

    layout_operator_lookup = context.get("__LAYOUT_OPERATOR__")
    if not layout_operator_lookup:
        logger.error("Layout operator not provided in context")
        return -1
    
    logger.debug(f"Layout operator: {layout_operator_lookup}")

    dataflow = context.get("__DATAFLOW__")
    container = context.get("__CONTAINER__")

    # Get the shell script name
    script_name = layout_operator_lookup(
        project_config, execution_config, dataflow, container, line.get("__PARAM0__"), **context
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
    connection_string = f"host={connection_parts.get('__PARAM3__')} port={connection_parts.get('__PARAM4__')} dbname={connection_parts.get('__PARAM5__')} user={connection_parts.get('__PARAM6__')} password={connection_parts.get('__PARAM7__')}"
    logger.info(f"Connection string: {connection_string}")

    # Construct the command
    command = [
        "psql",
        connection_string,
        "-f",
        script_name,
    ]
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
