import csv, logging, os, subprocess, time, threading
from csv import QUOTE_MINIMAL, reader as csv_reader
from p100.core import base64_encode, base64_decode

logger = logging.getLogger(__name__)


def get_list_from_file(file_path: str, comment: str = "#") -> list:
    """
    Read a file and return a list of lines excluding empty and commented lines.

    Args:
        file_path (str): Path to the file.
        comment (str): Character used to indicate a comment line. Default is '#'.

    Returns:
        list: List of lines excluding empty and commented lines from the file. Empty list if file is empty.
    """
    list_of_values = []
    if not file_path or os.path.isfile(file_path) == False:
        logger.error(f"File path is empty or file does not exist: {file_path}")
        return list_of_values

    all_lines = []
    try:
        with open(file_path, mode="r") as f:
            all_lines = list(f)
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return []

    logger.debug(f"Read {len(all_lines)} lines from file: {file_path}")
    for line in all_lines:
        # Skip empty lines and comments
        if line.isspace() or line.startswith(comment):
            continue
        list_of_values.append(line.strip())
    logger.debug(f"Read Valid Lines {len(list_of_values)} lines from file: {file_path}")
    return list_of_values


def get_list_from_csv(
    file_path: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = "\\",
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\n",
    quoting: int = QUOTE_MINIMAL,
    strict: bool = False,
    comment: str = "#",
) -> list:
    """
    Read a CSV file and return a list of values.

    Args:
        file_path (str): Path to the CSV file.
        delimiter (str): Delimiter used in the CSV file. Default is ','.
        quotechar (str): Character used to quote fields containing special characters. Default is '"'.
        escapechar (str): Character used to escape the delimiter if quoting is set to QUOTE_NONE. Default is None.
        doublequote (bool): Whether to double quote the quotechar if it appears in a field. Default is True.
        skipinitialspace (bool): Whether to skip spaces after the delimiter. Default is False.
        lineterminator (str): Character used to terminate lines. Default is '\n'.
        quoting (csv._QuotingType): Control when quotes should be generated or expected. Default is csv.QUOTE_MINIMAL.
        strict (bool): Whether to raise an error on invalid CSV format. Default is False.
        comment (str): Character used to indicate a comment line. Default is None.

    Returns:
        list: List of rows excluding empty and commented lines from the CSV file. Empty list if file is empty.
    """
    list_of_values = []
    if not file_path or os.path.isfile(file_path) == False:
        logger.error(f"File path is empty or file does not exist: {file_path}")
        return list_of_values

    resolved_delimiter = resolve_delimiter(delimiter)
    resolved_quotechar = resolve_delimiter(quotechar)
    resolved_escapechar = resolve_delimiter(escapechar)

    try:
        with open(file_path, mode="r") as f:
            reader = csv_reader(
                f,
                delimiter=resolved_delimiter,
                quotechar=resolved_quotechar,
                escapechar=resolved_escapechar,
                doublequote=doublequote,
                lineterminator=lineterminator,
                quoting=quoting,
                strict=strict,
                skipinitialspace=skipinitialspace,
            )
            for row in reader:
                # Skip empty lines and comments
                if not row or row[0].startswith(comment):
                    continue
                list_of_values.append(row)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return list_of_values

    return list_of_values


def resolve_delimiter(in_delimiter: str) -> str:
    """
    Resolve the delimiter from a string to its corresponding character.
    Args:
        in_delimiter (str): The delimiter string to resolve.
    Returns:
        str: The resolved delimiter character.
    """
    if not in_delimiter:
        logger.debug("No delimiter provided, using default")
        return ","

    logger.debug(f"Resolving delimiter: {in_delimiter}")

    delimiter = in_delimiter.upper()
    if delimiter == "TAB":
        delimiter = "\t"
    if delimiter == "COMMA":
        delimiter = ","
    if delimiter == "PIPE":
        delimiter = "|"
    if delimiter == "CARAT":
        delimiter = "^"
    if delimiter == "SPACE":
        delimiter = " "
    if delimiter == "SEMICOLON":
        delimiter = ";"
    if delimiter == "COLON":
        delimiter = ":"
    if delimiter == "DOLLAR":
        delimiter = "$"
    if delimiter == "HASH":
        delimiter = "#"
    if delimiter == "AT":
        delimiter = "@"
    if delimiter == "AMPERSAND":
        delimiter = "&"
    if delimiter == "ASTERISK":
        delimiter = "*"
    if delimiter == "PERCENT":
        delimiter = "%"
    if delimiter == "EXCLAMATION":
        delimiter = "!"
    if delimiter == "QUESTION":
        delimiter = "?"
    if delimiter == "TILDE":
        delimiter = "~"
    if delimiter == "BACKSLASH":
        delimiter = "\\"
    if delimiter == "FORWARDSLASH":
        delimiter = "/"
    if delimiter == "HYPHEN":
        delimiter = "-"
    if delimiter == "UNDERSCORE":
        delimiter = "_"
    if delimiter == "PLUS":
        delimiter = "+"
    if delimiter == "EQUAL":
        delimiter = "="
    if delimiter == "DOT":
        delimiter = "."
    if delimiter == "DOUBLEQUOTES":
        delimiter = '"'
    if delimiter == "SINGLEQUOTES":
        delimiter = "'"
    if delimiter == "NULL":
        delimiter = None
    if delimiter == "NONE":
        delimiter = None
    if delimiter == "EMPTY":
        delimiter = ""
    if delimiter == "DEFAULT":
        delimiter = ","

    logger.debug(f"Resolved delimiter: {delimiter}")
    return delimiter


def get_default_env_config(
    config: dict, operator_name: str, **context
) -> tuple[dict, dict]:
    """
    Get the default enviroment and values for a operator configuration.
    Args:
        config (dict): The configuration dictionary.
        operator_name (str): The name of the operator.
        **context: Additional context parameters.
    Returns:
        dict, dict: The default environment variables and values for the operator.
    """
    env_vars = {}
    default_values = {}
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

    return env_vars, default_values


def get_default_value(
    config: dict, operator_name: str, property_name: str, **context
) -> str | None:
    """
    Get the default value for a given property from the operator configuration. "

    Args:
        config (dict): The configuration dictionary.
        operator_name (str): The name of the operator.
        property_name (str): The property name to retrieve the default value for.
        **context: Additional context parameters.
    Returns:
        str | None: The default value for the property, or None if not found.
    """
    env_vars, default_values = get_default_env_config(config, operator_name, **context)
    return default_values.get(property_name, None)


def execute_cmd(
    command: str | list, env_vars: dict = None, input: str = None, output: str = None
) -> int:
    """
    Execute a command in a subprocess with optional environment variables and input/output redirection.
    Args:
        command (str | list): The command to execute.
        env_vars (dict): Optional environment variables to set for the subprocess.
        input (str): Optional input to send to the subprocess.
        output (str): Optional file path to redirect output to.
    Returns:
        int: The return code of the command execution.
    """
    now = int(time.time())
    logger.debug(f"Executing command: {command} with: {input} and output: {output}")
    execution_time = 0

    def print_dots():
        """
        Print a dot every second to indicate that the command is still running.
        """
        while not process_completed.is_set():
            print(".", end="", flush=True)
            time.sleep(1)

    process_completed = threading.Event()
    try:
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)

        process = subprocess.Popen(
            command,
            text=True,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        dots_thread = threading.Thread(target=print_dots)
        dots_thread.start()

        stdout, stderr = process.communicate(input=input if input else None)
        if process.returncode == 0 and output:
            with open(output, "w") as f:
                f.write(stdout)
        process_completed.set()
        dots_thread.join()

        end = int(time.time())
        execution_time = end - now
        logger.debug(f"error output: {stderr.strip()}")
        logger.debug(
            f"Command: {process.args} return code: {process.returncode} in: {execution_time} seconds"
        )
        return process.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing script: {e}")
        return -1
    except Exception as e:
        logger.error(f"Unexpected error executing command: {e}")
        return -1
    finally:
        end = int(time.time())
        execution_time = end - now
        logger.debug(f"Command execution completed in: {execution_time} seconds")
        print(f"{execution_time}s")
    return 0


def base64_encode(data: str) -> str:
    """
    Encode a string to Base64 format.
    Args:
        data (str): The string to encode.
    Returns:
        str: The Base64 encoded string.
    """
    return base64_encode(data)


def base64_decode(data: str) -> str:
    """
    Decode a Base64 encoded string.
    Args:
        data (str): The Base64 encoded string to decode.
    Returns:
        str: The decoded string.
    """
    return base64_decode(data)
