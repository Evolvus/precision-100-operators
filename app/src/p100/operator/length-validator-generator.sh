#!/bin/bash
# This script generates a SQL script that validates the length of each column in a table
# Usage: ./length-validator-generator.sh TABLE_NAME VIEW_NAME CONNECTION_STRING
# Example: ./length-validator-generator.sh MY_TABLE MY_VIEW "user/pass@//localhost:1521/xe"
TABLE_NAME=$1
VIEW_NAME=$2
CONNECTION_STRING=$3
lines="$( sqlplus -s /nolog <<EOF
CONNECT $CONNECTION_STRING
set feedback off
SET VERIFY OFF
set head off
set markup csv on quote off
select column_name,data_length
from   o_tab_columns
where  table_name= upper('$TABLE_NAME')
and    data_length > 0;
exit;
EOF
)"
line_count=`echo $lines | wc -w `

counter=0;
echo "INSERT INTO ${VIEW_NAME} "
for line in $lines; do
  counter=$counter+1;
  COLUMN_NAME=$(echo $line | cut -d',' -f 1)
  DATA_LENGTH=$(echo $line | cut -d',' -f 2)
  echo "SELECT 'INVALID $COLUMN_NAME LENGTH', COUNT(1) FROM $TABLE_NAME WHERE LENGTH($COLUMN_NAME) > $DATA_LENGTH "
  if [[ $counter -eq $line_count ]]; then
    echo ";"
  else
    echo "UNION "
  fi
done