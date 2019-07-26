export MAP_FILE_WORK_FOLDER=$PRECISION100_EXECUTION_FOLDER/map-file/work
export MAP_FILE_FILE_SUFFIX="tsv"
export MAP_FILE_JOIN_FILTER="__JOIN__"

export DEFAULT_TABLE_NAME_PREFIX="O"
export DEFAULT_REVERSE_TABLE_NAME_PREFIX="R"
export DEFAULT_TAB_COLUMN_SUFFIX="tab-column"
export DEFAULT_TRANSFORM_SUFFIX="transform"
export DEFAULT_COLUMN_DATA_TYPE="NVARCHAR2(2000)"

export DEFAULT_COLUMN_NAME_INDEX="1"
export DEFAULT_DATA_TYPE_INDEX="2"
export DEFAULT_MAX_LENGTH_INDEX="3"
export DEFAULT_MAPPING_TYPE_INDEX="4"
export DEFAULT_MAPPING_VALUE_INDEX="5"
#
# To make tab or any non printable character the delimiter use the expression
# 
# export DEFAULT_MAP_FILE_DELIMITER=$'\t'
#
# for others like ',' or '~' etc, we can directly assign the value as below
# export DEFAULT_MAP_FILE_DELIMITER=',' 
#
export DEFAULT_MAP_FILE_DELIMITER='~'
