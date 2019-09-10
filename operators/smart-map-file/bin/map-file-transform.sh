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

#
# $1 - counter
# $2 - column name
#
function insert_column_defn() {
  column="${2:0:30}"

  if [[ $1 -eq 1 ]]; then
     echo "  $column"
  else
     echo ", $column"
  fi
}

#
# $1 - counter
# $2 - column name
# $3 - mapping code
# $4 - mapping value
# 
function select_column_defn() {
  echo " -- $2"
  case "$3" in
   'CONSTANT')
     column="'$4'"
    ;;
   'PASSTHRU')
     column="$4"
    ;;
   *)
     column="NULL"
    ;;
  esac

  if [[ $1 -eq 1 ]]; then
    echo "  $column"
  else
    echo ", $column"
  fi
}


#
# $1 - SOURCE_FILE
# $2 - mode INSERT | SELECT
#
function column_loop() {
  counter=0
  while IFS='~' read -r column_name old_column_name data_type max_length mapping_code mapping_value justification mandatory info1 info2 info3;
  do
    if [[ -z "$column_name" ]]; then
      continue;
    fi
    if [[ counter -eq 0 ]]; then
      counter=$counter+1;
      continue;
    fi

    case "$2" in
     'INSERT')
       insert_column_defn $counter $column_name
      ;;
     'SELECT')
       select_column_defn $counter $column_name $mapping_code $mapping_value
      ;;
     *)
       echo "Invalid looping mode"
       echo "Error: Invalid looping mode" 1>&2
      ;;
    esac

    counter=$counter+1;
  done < <(cat ${1} | tr '\t' '~' | tr -d '\r' | grep .)
}

echo "EXEC TRANSFORM_INTERCEPTOR('PRE','${TABLE_NAME}'); "
echo "INSERT INTO ${TABLE_NAME_PREFIX}_${TABLE_NAME} ("
column_loop $SOURCE_FILE 'INSERT'
echo ") SELECT "
column_loop $SOURCE_FILE 'SELECT'

#
# Now add the where clause for the select 
#

while IFS='~' read -r mapping_value;
do
  if [[ -z "$mapping_value" ]]; then
    continue;
  fi
  echo "$mapping_value"
done < <( cat ${JOIN_FILE} | tr -d '\t' | tr -d '\r' | grep .)
echo ";"
echo "EXEC TRANSFORM_INTERCEPTOR('POST','${TABLE_NAME}'); "
