#!/bin/bash

TABLE_NAME_PREFIX=${DEFAULT_TABLE_NAME_PREFIX:-O}
FILE_NAME_SUFFIX=${DEFAULT_FILE_NAME_SUFFIX:-csv}

MAP_FILE_DELIMITER=${DEFAULT_MAP_FILE_DELIMITER:-~}

FILE_NAME=$1
MAPPED_TABLE_NAME="${TABLE_NAME_PREFIX}_${TABLE_NAME}"

echo "DELETE FROM MASKING_MAPPER;"

counter=0
while IFS='~' read -r schema_name table_name column_name phase mapping_method update_where;
do
  if [[ -z "$column_name" ]]; then
     continue;
  fi
  if [[ counter -eq 0 ]]; then
    counter=$counter+1;
    continue;
  fi
  echo "INSERT INTO MASKING_MAPPER ( SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, PHASE, MAPPING_METHOD, UPDATE_WHERE ) VALUES ( UPPER('$schema_name'), UPPER('${table_name}'), UPPER('$column_name'), UPPER('$phase'), UPPER('$mapping_method'), UPPER('$update_where') );"
done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)
