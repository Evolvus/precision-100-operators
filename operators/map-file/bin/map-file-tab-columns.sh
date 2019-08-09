#!/bin/bash

TABLE_NAME_PREFIX=${DEFAULT_TABLE_NAME_PREFIX:-O}
FILE_NAME_SUFFIX=${DEFAULT_FILE_NAME_SUFFIX:-csv}

MAP_FILE_DELIMITER=${DEFAULT_MAP_FILE_DELIMITER:-~}

TABLE_NAME=$1
SOURCE_FILE=$2

MAPPED_TABLE_NAME="${TABLE_NAME_PREFIX}_${TABLE_NAME}"

echo "DELETE FROM O_TAB_COLUMNS WHERE TABLE_NAME = UPPER('${MAPPED_TABLE_NAME}');"

counter=0
while IFS='~' read -r column_name data_type max_length mapping_code mapping_value;
do
  if [[ -z "$column_name" ]]; then
     continue;
  fi
  if [[ counter -eq 0 ]]; then
    counter=$counter+1;
    continue;
  fi
  if [[ -z $max_length ]]; then
     max_length=0
  fi
  echo "INSERT INTO O_TAB_COLUMNS ( TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH ) VALUES ( UPPER('${MAPPED_TABLE_NAME}'), UPPER('$column_name'), UPPER('$data_type'), $max_length);"
done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)
