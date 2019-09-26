MASKINIG_MAPPER_WORK_FOLDER=$PRECISION100_EXECUTION_FOLDER/masking-mapper/work

MAP_FILE_FILE_SUFFIX="tsv"

DEFAULT_COLUMN_NAME_INDEX="1"
DEFAULT_DATA_TYPE_INDEX="2"
DEFAULT_MAX_LENGTH_INDEX="3"
DEFAULT_MAPPING_TYPE_INDEX="4"
DEFAULT_MAPPING_VALUE_INDEX="5"
#
# To make tab or any non printable character the delimiter use the expression
# 
# export DEFAULT_MAP_FILE_DELIMITER=$'\t'
#
# for others like ',' or '~' etc, we can directly assign the value as below
# export DEFAULT_MAP_FILE_DELIMITER=',' 
#
DEFAULT_MAP_FILE_DELIMITER='~'
