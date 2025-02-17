import subprocess, threading
import logging

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
