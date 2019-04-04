#!/bin/bash

TABLE_NAME=$1
DATA_FILE_SEPARATOR=$2
CONNECTION_STRING=$3

if [ -z "$CHAR_SET" ]; then
    CHAR_SET=${DEFAULT_CHARSET:-UTF8}
fi
if [ -z "$DATA_FILE_SEPARATOR" ]; then
    DATA_FILE_SEPARATOR=${DEFAULT_DATA_FILE_SEPARATOR:-,}
fi

lines="$( sqlplus -s /nolog <<EOF
CONNECT $CONNECTION_STRING
set feedback off
SET VERIFY OFF
set head off
select column_name
from   user_tab_columns
where  table_name= upper('$TABLE_NAME')
order by column_id;
exit;
EOF
)"
line_count=`echo $lines | wc -w `

counter=0;
echo "LOAD DATA"
echo "CHARACTERSET $CHAR_SET"
echo "INFILE *"
echo "TRUNCATE INTO TABLE $1"
echo "FIELDS TERMINATED BY '$DATA_FILE_SEPARATOR'"
echo "TRAILING NULLCOLS ("
for line in $lines; do
  counter=$counter+1;
  if [[ $counter -eq $line_count ]]; then
    echo $line;
  else
    echo "$line,"
  fi;
done;
echo ")"
