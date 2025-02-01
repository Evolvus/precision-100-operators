import os, logging
import csv

PARAM_PROJECT_REG_FILE_URL = "project_reg_file"
PARAM_ENV = "env"

logger = logging.getLogger(__name__)

class NativeLayoutOperator:
    def get_dataflows(self, **context):
        logger.info(f"Getting dataflows with context: {context}")
        my_env = context[PARAM_ENV]

        #Get the value of the PRECISION100_PROJECT_CONF_FOLDER from my_env
        conf_folder = my_env.get("PRECISION100_EXECUTION_DATAFLOW_FOLDER")
        if not conf_folder:
            logger.error("Environment variable PRECISION100_PROJECT_CONF_FOLDER is not set in the provided environment context")
            return {}

        project_reg_file = os.path.join(conf_folder, "project.reg")

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
