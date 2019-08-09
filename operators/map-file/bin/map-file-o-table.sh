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

echo "DROP TABLE ${TABLE_NAME_PREFIX}_${TABLE_NAME};"
echo "CREATE TABLE ${TABLE_NAME_PREFIX}_${TABLE_NAME} ("
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
  if [[ counter -eq 1 ]]; then
    counter=$counter+1;
    echo "  $column_name $COLUMN_DATA_TYPE"
    continue;
  fi
  counter=$counter+1;
  echo ", $column_name $COLUMN_DATA_TYPE"
done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)
echo ");"

# Create the r tables
echo "DROP TABLE ${REVERSE_TABLE_NAME_PREFIX}_${TABLE_NAME};"
echo "CREATE TABLE ${REVERSE_TABLE_NAME_PREFIX}_${TABLE_NAME} ("
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
  if [[ counter -eq 1 ]]; then
    counter=$counter+1;
    echo "  $column_name $COLUMN_DATA_TYPE"
    continue;
  fi
  counter=$counter+1;
  echo ", $column_name $COLUMN_DATA_TYPE"
done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)
echo ");"
