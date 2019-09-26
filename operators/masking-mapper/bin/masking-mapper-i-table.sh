#!/bin/bash

MAP_FILE_DELIMITER=${DEFAULT_MAP_FILE_DELIMITER:-~}

SOURCE_FILE=$1

echo "DELETE FROM MASKING_MAPPER;"

counter=0
while IFS='~' read -r schema_name table_name column_name phase mapping_method mapping_value update_where enabled_flag;
do
  if [[ -z "$column_name" ]]; then
     continue;
  fi
  if [[ counter -eq 0 ]]; then
    counter=$counter+1;
    continue;
  fi
  if [[ -z $enabled_flag ]]; then
    enabled_flag='Y'
  fi
  echo "INSERT INTO MASKING_MAPPER ( SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, PHASE, MAPPING_METHOD, MAPPING_VALUE, UPDATE_WHERE, ENABLED ) VALUES ( UPPER('${schema_name}'), UPPER('${table_name}'), UPPER('${column_name}'), UPPER('${phase}'), UPPER('${mapping_method}'), UPPER('${mapping_value}'), UPPER('${update_where}'), UPPER('${enabled_flag}') );"
done < <(cat ${SOURCE_FILE} | tr '\t' '~' | tr -d '\r' | grep .)
