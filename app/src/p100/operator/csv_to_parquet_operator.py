import logging
from .cmdline_operator import execute_cmd, get_default_value, resolve_delimiter

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

    # Get the csv file name
    csv_file_name = layout_operator_lookup(
        project_config, execution_config, dataflow, container, line.get("__PARAM0__"), **context
    )

    parquet_file_name = layout_operator_lookup(
        project_config, execution_config, dataflow, container, line.get("__PARAM1__"), **context
    )
    delimiter = get_default_value("DELIMITER", "CSV2PARQUET", **context) 
    quote = get_default_value("QUOTE", "CSV2PARQUET", **context)
    escape = get_default_value("ESCAPE", "CSV2PARQUET", **context)
    header = get_default_value("HEADER", "CSV2PARQUET", **context)
    ignore_errors = get_default_value("IGNORE_ERRORS", "CSV2PARQUET", **context)

    if line.get("__PARAM2__"):
        delimiter = line.get("__PARAM2__", delimiter)
    delimiter = resolve_delimiter(delimiter)

    if line.get("__PARAM3__"):
        quote = line.get("__PARAM3__", quote)
    quote = resolve_delimiter(quote)

    if line.get("__PARAM4__"):
        escape = line.get("__PARAM4__", escape)
    escape = resolve_delimiter(escape)

    if line.get("__PARAM5__") and line.get("__PARAM5__").upper() == "NO":
        header = False
    else:
        header = True

    if line.get("__PARAM6__") and line.get("__PARAM6__").upper() == "NO":
        ignore_errors = False
    else:   
        ignore_errors = True

    if line.get("__PARAM7__"):
        columns = line.get("__PARAM7__")
    
    if columns:
        script_name = f"copy (select * from read_csv('{csv_file_name}', delim = '{delimiter}', quote = '{quote}', header = {header}, escape = '{escape}', ignore_errors = {ignore_errors}, columns = ({columns}) )) to '{parquet_file_name}' (format parquet)"
    else:
        script_name = f"copy (select * from read_csv('{csv_file_name}', delim = '{delimiter}', quote = '{quote}', header = {header}, escape = '{escape}', ignore_errors = {ignore_errors})) to '{parquet_file_name}' (format parquet)"

    # Construct the command
    command = [
        "duckdb",
        "-c",
        script_name,
    ]
    return execute_cmd(command)