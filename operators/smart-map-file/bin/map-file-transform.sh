#!/bin/bash

TABLE_NAME_PREFIX=${DEFAULT_TABLE_NAME_PREFIX:-O}
COLUMN_NAME_INDEX=${DEFAULT_COLUMN_NAME_INDEX:-1}
DATA_TYPE_INDEX=${DEFAULT_DATA_TYPE_INDEX:-2}
MAX_LENGTH_INDEX=${DEFAULT_MAX_LENGTH_INDEX:-4}
MAPPING_TYPE_INDEX=${DEFAULT_MAPPING_TYPE_INDEX:-7}
MAPPING_VALUE_INDEX=${DEFAULT_MAPPING_VALUE_INDEX:-8}
MAP_FILE_DELIMITER=${DEFAULT_MAP_FILE_DELIMITER:-~}

TABLE_NAME=$1
SOURCE_FILE=$2
JOIN_FILE=$3

echo "EXEC TRANSFORM_INTERCEPTOR('PRE','${TABLE_NAME}'); "
echo "INSERT INTO ${TABLE_NAME_PREFIX}_${TABLE_NAME} ("
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
    echo "  ${column_name:0:30}"
    continue;
  fi
  counter=$counter+1;
  echo ", ${column_name:0:30}"
done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)
echo ") SELECT "

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

  echo " -- $column_name"
  case "$mapping_code" in
   'CONSTANT')
     column="'$mapping_value'"
    ;;
   'PASSTHRU')
     column="$mapping_value"
    ;;
   *)
     column="NULL"
    ;;
  esac

  if [[ counter -eq 1 ]]; then
    echo "  $column"
  else
    echo ", $column"
  fi
  counter=$counter+1;
done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)

while IFS='~' read -r mapping_value;
do
  if [[ -z "$mapping_value" ]]; then
    continue;
  fi
  echo "$mapping_value"
done < <( cat ${JOIN_FILE} | tr -d '\t' | tr -d '\r' | grep .)
echo ";"
echo "EXEC TRANSFORM_INTERCEPTOR('POST','${TABLE_NAME}'); "
