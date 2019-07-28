#!/bin/bash

REVERSE_TABLE_NAME_PREFIX=${DEFAULT_REVERSE_TABLE_NAME_PREFIX:-R}
TABLE_NAME_PREFIX=${DEFAULT_TABLE_NAME_PREFIX:-O}
FILE_NAME_SUFFIX=${DEFAULT_FILE_NAME_SUFFIX:-csv}
COLUMN_DATA_TYPE=${DEFAULT_COLUMN_DATA_TYPE:-NVARCHAR2(2000)}
COLUMN_NAME_INDEX=${DEFAULT_COLUMN_NAME_INDEX:-1}
DATA_TYPE_INDEX=${DEFAULT_DATA_TYPE_INDEX:-2}
MAX_LENGTH_INDEX=${DEFAULT_MAX_LENGTH_INDEX:-3}
MAP_FILE_DELIMITER=${DEFAULT_MAP_FILE_DELIMITER:-~}

TABLE_NAME=$1
SOURCE_FILE=$2

function get_column_definition() {
  v_column_name=$(echo ${1:0:30} | tr '[:lower:]' '[:upper:]')
  v_data_type="NVARCHAR2"
  v_column_length=${2:-2000}

  echo " $v_column_name $v_data_type($v_column_length)"
}

function define_table() {
  upper_case_table_name=$(echo "${1}_${2}" | tr '[:lower:]' '[:upper:]')
  echo "DROP TABLE ${upper_case_table_name};"
  echo "CREATE TABLE ${upper_case_table_name} ("
  counter=0
  while IFS='~' read -r column_name old_column_name data_type max_length mapping_code mapping_value justification mandatory;
  do
    if [[ -z "$column_name" ]]; then
      continue;
    fi
    if [[ counter -eq 0 ]]; then
      counter=$counter+1;
      continue;
    fi
    if [[ counter -eq 1 ]]; then
      counter=$counter+1;
      get_column_definition "$column_name" $max_length
      continue;
    fi
    counter=$counter+1;
    echo ", $(get_column_definition "$column_name" $max_length)"
  done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)
  echo ");"
}

define_table ${TABLE_NAME_PREFIX} ${TABLE_NAME}
define_table ${REVERSE_TABLE_NAME_PREFIX} ${TABLE_NAME}
