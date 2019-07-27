#!/bin/bash

TABLE_NAME=$1
DELIMITER=$2
QUOTE=$3
SPOOL_FILE=$4
CONNECTION_STRING=$5

sqlplus -s /nolog  <<EOF >> /dev/null
connect $CONNECTION_STRING
set head off
set feedback off
set term off
set pages 0
set trim on
set verify off
set markup csv on delimiter $DELIMITER quote $QUOTE
spool $SPOOL_FILE
SELECT * from $TABLE_NAME;
spool off;
exit;

EOF
