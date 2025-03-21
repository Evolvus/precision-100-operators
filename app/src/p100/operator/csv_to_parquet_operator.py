import logging
from .cmdline_operator import execute_cmd


logger = logging.getLogger(__name__)

def execute(project_config, execution_config, line, **context):
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

    # Get the scsv file name
    csv_file_name = layout_operator_lookup(
        project_config, execution_config, dataflow, container, line.get("__PARAM0__"), **context
    )

    parquet_file_name = layout_operator_lookup(
        project_config, execution_config, dataflow, container, line.get("__PARAM1__"), **context
    )

    script_name = f"copy (select * from read_csv('{csv_file_name}')) to '{parquet_file_name}' (format parquet)"

    # Construct the command
    command = [
        "duckdb",
        "-c",
        script_name,
    ]
    return execute_cmd(command)