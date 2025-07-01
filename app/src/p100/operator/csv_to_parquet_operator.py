import logging
import os
from .cmdline_operator import get_default_value
from .operator_utils import resolve_delimiter, execute_cmd

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

    # check if the file actually exists and is non zero length
    if not os.path.isfile(csv_file_name) or os.path.getsize(csv_file_name) == 0:
        logger.error(f"CSV file {csv_file_name} does not exist or is empty.")
        return 0

    parquet_file_name = layout_operator_lookup(
        project_config, execution_config, dataflow, container, line.get("__PARAM1__"), **context
    )
    delimiter = get_default_value("DELIMITER", "CSV2PARQUET", **context) 
    quote = get_default_value("QUOTE", "CSV2PARQUET", **context)
    escape = get_default_value("ESCAPE", "CSV2PARQUET", **context)
    header = get_default_value("HEADER", "CSV2PARQUET", **context)
    ignore_errors = get_default_value("IGNORE_ERRORS", "CSV2PARQUET", **context)
    strict_mode = get_default_value("STRICT_MODE", "CSV2PARQUET", **context)
    memory_limit = get_default_value("MEMORY_LIMIT", "CSV2PARQUET", **context)
    no_of_threads = get_default_value("NO_OF_THREADS", "CSV2PARQUET", **context)

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

    if line.get("__PARAM6__") and line.get("__PARAM6__").upper() == "YES":
        ignore_errors = True
        store_rejects = False
    else:   
        ignore_errors = False

    if line.get("__PARAM7__") and line.get("__PARAM7__").upper() == "NO":
        store_rejects = False
    else:
        store_rejects = True

    if line.get("__PARAM8__"):
        columns = line.get("__PARAM8__")

    if line.get("__PARAM9__") and line.get("__PARAM9__").upper() == "NO":
        strict_mode = False
    else:
        strict_mode = True

    if line.get("__PARAM10__") and line.get("__PARAM10__").upper() == "YES":
        ignore_errors = True
        store_rejects = False
    else:
        ignore_errors = False

    script_name = "set memory_limit = '{memory_limit}'; set threads = {no_of_threads}; set preserve_insertion_order = false;"
    if columns:
        script_name = f"copy (select * from read_csv('{csv_file_name}', strict_mode = {strict_mode}, delim = '{delimiter}', quote = '{quote}', header = {header}, escape = '{escape}', store_rejects = {store_rejects}, columns = ({columns}) )) to '{parquet_file_name}' (format parquet);"
    else:
        script_name = f"copy (select * from read_csv('{csv_file_name}', strict_mode = {strict_mode}, delim = '{delimiter}', quote = '{quote}', header = {header}, escape = '{escape}', store_rejects = {store_rejects})) to '{parquet_file_name}' (format parquet);"
    
    if store_rejects:
        script_name += f"copy reject_scans to '{parquet_file_name}.reject_scans' (HEADER, DELIMITER ',');"
        script_name += f"copy reject_errors to '{parquet_file_name}.reject_errors' (HEADER, DELIMITER ',');" 

    print(f"Compress {line.get('__PARAM0__')}")
    # Construct the command
    command = [
        "duckdb",
        "-c",
        script_name,
    ]
    return execute_cmd(command)