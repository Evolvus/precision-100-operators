import os, logging
import csv

PARAM_PROJECT_REG_FILE_URL = "project_reg_file"
PARAM_ENV = "env"

logger = logging.getLogger(__name__)

class NativeLayoutOperator:
    def get_dataflows(self, **context):
        logger.info(f"Getting dataflows")
        logger.debug(f"Getting dataflows with context: {context}")
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


    def get_containers(self, dataflow, **context):
        logger.info(f"Getting containers for dataflow: {dataflow}")
        logger.debug(f"Getting containers for dataflow: {dataflow} with context: {context}")
        my_env = context[PARAM_ENV]

        dataflow_reg = dataflow.split(",")[1]

        #Get the value of the PRECISION100_EXECUTION_DATAFLOW_FOLDER from my_env
        dataflow_folder = my_env.get("PRECISION100_EXECUTION_DATAFLOW_FOLDER")
        if not dataflow_folder:
            logger.error("Environment variable PRECISION100_EXECUTION_DATAFLOW_FOLDER is not set in the provided environment context")
            return []

        dataflow_reg_file = os.path.join(dataflow_folder, f"{dataflow_reg}.reg")

        if not os.path.isfile(dataflow_reg_file):
            logger.error(f"Project reference file does not exist: {dataflow_reg_file}")
            return []

        container_list = []
        with open(dataflow_reg_file, "r") as f:
            for line in f:
                logger.info(f"Container reference: {line.strip()}")
                container_list.append(line.strip())

        return container_list


    def get_instructions(self, dataflow, container, **context):
        logger.info(f"Getting instructions for dataflow: {dataflow} container: {container}")
        logger.debug(f"Getting instructions for dataflow: {dataflow} container: {container} with context: {context}")
        my_env = context[PARAM_ENV]

        #Get the value of the PRECISION100_EXECUTION_CONTAINER_FOLDER from my_env
        container_folder = my_env.get("PRECISION100_EXECUTION_CONTAINER_FOLDER")
        if not container_folder:
            logger.error("Environment variable PRECISION100_EXECUTION_CONTAINER_FOLDER is not set in the provided environment context")
            return []

        container_reg_file = os.path.join(container_folder, container, "container.reg")

        if not os.path.isfile(container_reg_file):
            logger.error(f"Project reference file does not exist: {container_reg_file}")
            return []

        instruction_list = []
        with open(container_reg_file, "r") as f:
            for line in f:
                logger.info(f"Instruction: {line.strip()}")
                instruction = self.get_instruction(dataflow, container, line.strip(), **context)
                logger.info(f"Instruction: {instruction}")
                instruction_list.append(instruction)

        return instruction_list

    def get_instruction(self, dataflow, container, instruction, **context):
        logger.info(f"Getting instruction for dataflow: {dataflow} container: {container} instruction: {instruction}")
        logger.debug(f"Getting instruction for dataflow: {dataflow} container: {container} instruction: {instruction} with context: {context}")

        instruction_parts = instruction.strip().split(',')
        if len(instruction_parts) < 3:
           logger.error(f"Invalid instruction line: {instruction.strip()}")
           return {}

        instruction_context = {
           "__ORDER__": instruction_parts[0],
           "__PARAM0__": instruction_parts[1],
           "__OPERATOR_NAME__": instruction_parts[2]
        }

        for i, param in enumerate(instruction_parts[3:], start=1):
           instruction_context[f"__PARAM{i}__"] = param
            
        return instruction_context


    def lookup(self, dataflow, container, file, **context):
        logger.info(f"Lookup {file} for dataflow: {dataflow} container: {container}")
        logger.debug(f"Lookup {file} for dataflow: {dataflow} container: {container} with context: {context}")
        my_env = context[PARAM_ENV]


        project_folder = my_env.get("PRECISION100_PROJECT_FOLDER")
        container_folder = my_env.get("PRECISION100_EXECUTION_CONTAINER_FOLDER")
        output_folder = my_env.get("PRECISION100_EXECUTION_OUTPUT_FOLDER")

        if file.startswith("project://"):
            norm_file_path = os.path.normpath(file[10:])
            path = os.path.join(project_folder, norm_file_path)
            # if path is not below project folder, return None
            if not path.startswith(project_folder):
                logger.error(f"Invalid project path: {path}")
                return ""
            
            logger.info(f"Lookup project path: {path}")
            return str(path)
        
        if file.startswith("output://"):
            norm_file_path = os.path.normpath(file[9:])
            path = os.path.join(output_folder, norm_file_path)
            if not path.startswith(output_folder):
                logger.error(f"Invalid output path: {path}")
                return ""

            logger.info(f"Lookup output path: {path}")
            return str(path)
        
        if file.startswith("container://"):
            norm_file_path = os.path.normpath(file[12:])
            path = os.path.join(container_folder, container, norm_file_path)
            if not path.startswith(container_folder):
                logger.error(f"Invalid container path: {path}")
                return ""
            
            logger.info(f"Lookup path: {path}")
            return str(path)

        # if not prefix is provided, assume it is a container file
        norm_file_path = os.path.normpath(file)
        path = os.path.join(container_folder, container, norm_file_path)
        logger.info(f"Lookup path: {path}")
        return str(path)
