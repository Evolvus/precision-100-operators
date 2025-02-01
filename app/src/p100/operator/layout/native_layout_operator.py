import os, logging
import csv

PARAM_PROJECT_REG_FILE_URL = "project_reg_file"
PARAM_OPERATION_MODE = "operation_mode"

logger = logging.getLogger(__name__)

class NativeLayoutOperator:
    def get_dataflows(self, **context):
     # Get the context
        project_reg_file = context[PARAM_PROJECT_REG_FILE_URL]
        operation_mode = context[PARAM_OPERATION_MODE]

        logger.info(f"Reading project reference file: {project_reg_file} for operation mode: {operation_mode}")

        if not os.path.isfile(project_reg_file):
            logger.error(f"Project reference file does not exist: {project_reg_file}")
            return {}

        project_data = {}
        with open(project_reg_file, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) != 2:
                    logger.error(f"Invalid row in project reference file: {row}")
                    continue
                description, data_flow_id = row
                project_data[description] = data_flow_id
                logger.info(f"Project reference: {description} -> {data_flow_id}")

        return project_data
