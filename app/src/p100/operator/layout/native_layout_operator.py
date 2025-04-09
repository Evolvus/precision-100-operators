import os, logging
import csv
from p100.operator.operator_utils import get_list_from_csv, get_list_from_file

PARAM_PROJECT_REG_FILE_URL = "project_reg_file"
PARAM_ENV = "env"

logger = logging.getLogger(__name__)

class NativeLayoutOperator:

    def get_dataflows(self, project_config, execution_config, **context):
        logger.debug(f"Getting dataflows")
        logger.debug(f"Getting dataflows with context: {context}")

        #Get the value of the PRECISION100_PROJECT_CONF_FOLDER from my_env
        conf_folder = execution_config.get("PRECISION100_EXECUTION_DATAFLOW_FOLDER")
        if not conf_folder:
            logger.error("Environment variable PRECISION100_PROJECT_CONF_FOLDER is not set in the provided environment context")
            return {}

        project_reg_file = os.path.join(conf_folder, "project.reg")

        if not os.path.isfile(project_reg_file):
            logger.error(f"Project reference file does not exist: {project_reg_file}")
            return {}

        project_data = {}
        project_data_list = get_list_from_csv(project_reg_file)
        if len(project_data_list) == 0:
            logger.error(f"Project reference file is empty: {project_reg_file}")
            return project_data

        for row in project_data_list:
            if len(row) != 2:
                logger.error(f"Invalid row in project reference file: {row}")
                continue
            description, data_flow_id = row
            project_data[description] = data_flow_id
            logger.debug(f"Project reference: {description} -> {data_flow_id}")

        return project_data


    def get_containers(self, project_config, execution_config, dataflow, **context):
        logger.debug(f"Getting containers for dataflow: {dataflow}")
        logger.debug(f"Getting containers for dataflow: {dataflow} with context: {context}")

        dataflow_reg = dataflow.split(",")[1]

        #Get the value of the PRECISION100_EXECUTION_DATAFLOW_FOLDER from my_env
        dataflow_folder = execution_config.get("PRECISION100_EXECUTION_DATAFLOW_FOLDER")
        if not dataflow_folder:
            logger.error("Environment variable PRECISION100_EXECUTION_DATAFLOW_FOLDER is not set in the provided environment context")
            return []

        dataflow_reg_file = os.path.join(dataflow_folder, f"{dataflow_reg}.reg")

        if not os.path.isfile(dataflow_reg_file):
            logger.error(f"Dataflow file does not exist: {dataflow_reg_file}")
            return []

        container_list = get_list_from_file(dataflow_reg_file)
        logger.debug(f"Containers to execute: {len(container_list)}")
        if len(container_list) == 0:
            logger.error(f"Dataflow file is empty: {dataflow_reg_file}")
            return []

        return container_list


    def get_instructions(self, project_config, execution_config, dataflow, container, **context):
        logger.debug(f"Getting instructions for dataflow: [{dataflow}] container: [{container}]")
        logger.debug(f"Getting instructions for dataflow: {dataflow} container: {container} with context: {context}")

        #Get the value of the PRECISION100_EXECUTION_CONTAINER_FOLDER
        container_folder = execution_config.get("PRECISION100_EXECUTION_CONTAINER_FOLDER")
        if not container_folder:
            logger.error("Environment variable PRECISION100_EXECUTION_CONTAINER_FOLDER is not set in the provided environment context")
            return []

        container_reg_file = os.path.join(container_folder, container, "container.reg")

        if not os.path.isfile(container_reg_file):
            logger.error(f"Container file does not exist: {container_reg_file}")
            return []

        result= []
        instruction_list = get_list_from_file(container_reg_file)
        logger.debug(f"Instructions to execute: {len(instruction_list)}")
        if len(instruction_list) == 0:
            logger.error(f"Container file is empty: {container_reg_file}")
            return []

        for line in instruction_list:
            instruction = self.get_instruction(project_config, execution_config, dataflow, container, line, **context)
            logger.debug(f"Instruction: {instruction}")
            result.append(instruction)

        logger.debug(f"Instructions to execute: {len(instruction_list)}")
        return result

    def get_instruction(self, project_config, execution_config, dataflow, container, instruction, delimiter=",", **context):
        logger.debug(f"Getting instruction for dataflow: [{dataflow}] container: [{container}] instruction: [{instruction}]")
        logger.debug(f"Getting instruction for dataflow: [{dataflow}] container: [{container}] instruction: [{instruction}] with context: {context}")

        instruction_parts = instruction.split(delimiter)
        if len(instruction_parts) < 3:
           logger.error(f"Invalid instruction line: {instruction}")
           return {}

        instruction_context = {
           "__ORDER__": instruction_parts[0],
           "__PARAM0__": instruction_parts[1],
           "__OPERATOR_NAME__": instruction_parts[2]
        }

        for i, param in enumerate(instruction_parts[3:], start=1):
           instruction_context[f"__PARAM{i}__"] = param
            
        logger.debug(f"Instruction context: {instruction_context}")
        return instruction_context


    def lookup(self, project_config, execution_config, dataflow, container, file, **context):
        logger.debug(f"Lookup {file} for dataflow: {dataflow} container: {container}")
        logger.debug(f"Lookup {file} for dataflow: {dataflow} container: {container} with context: {context}")
        my_env = context[PARAM_ENV]


        project_folder = project_config.get("PRECISION100_PROJECT_FOLDER")
        container_folder = execution_config.get("PRECISION100_EXECUTION_CONTAINER_FOLDER")
        output_folder = execution_config.get("PRECISION100_EXECUTION_OUTPUT_FOLDER")
        temp_folder = execution_config.get("PRECISION100_EXECUTION_TEMP_FOLDER")

        if file.startswith("temp://"):
            norm_file_path = os.path.normpath(file[7:])
            path = os.path.join(temp_folder, norm_file_path)
            if not path.startswith(temp_folder):
                logger.error(f"Invalid temp path: {path}")
                return ""
            
            logger.debug(f"Lookup temp path: {path}")
            return str(path)

        if file.startswith("project://"):
            norm_file_path = os.path.normpath(file[10:])
            path = os.path.join(project_folder, norm_file_path)
            # if path is not below project folder, return None
            if not path.startswith(project_folder):
                logger.error(f"Invalid project path: {path}")
                return ""
            
            logger.debug(f"Lookup project path: {path}")
            return str(path)
        
        if file.startswith("output://"):
            norm_file_path = os.path.normpath(file[9:])
            path = os.path.join(output_folder, norm_file_path)
            if not path.startswith(output_folder):
                logger.error(f"Invalid output path: {path}")
                return ""

            logger.debug(f"Lookup output path: {path}")
            return str(path)
        
        if file.startswith("container://"):
            norm_file_path = os.path.normpath(file[12:])
            path = os.path.join(container_folder, container, norm_file_path)
            if not path.startswith(container_folder):
                logger.error(f"Invalid container path: {path}")
                return ""
            
            logger.debug(f"Lookup path: {path}")
            return str(path)

        # if not prefix is provided, assume it is a container file
        norm_file_path = os.path.normpath(file)
        path = os.path.join(container_folder, container, norm_file_path)
        logger.debug(f"Lookup path: {path}")
        return str(path)
