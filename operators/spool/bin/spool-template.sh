CONTAINER=$1
INDEX=$(echo $2 | cut -d ',' -f 1)
FILE_NAME=$(echo $2 | cut -d ',' -f 2)
FILE_TYPE=$(echo $2 | cut -d ',' -f 3)
DELIMITER=$(echo $2 | cut -d ',' -f 4)
QUOTE=$(echo $2 | cut -d ',' -f 5)
SPOOL_FILE_NAME=$(echo $2 | cut -d ',' -f 6)
CONNECTION_NAME=$(echo $2 | cut -d ',' -f 7)

if test "$SIMULATION_MODE" = "TRUE"; then
   echo "        START SPOOL ADAPTOR $FILE_NAME"
   sleep $SIMULATION_SLEEP;
   echo "        END SPOOL ADAPTOR $FILE_NAME"
   exit;
fi

echo "        START SPOOL ADAPTOR $FILE_NAME"

source $PRECISION100_OPERATORS_FOLDER/spool/conf/.spool.env.sh

if [ -z "$DELIMITER" ]; then
    DELIMITER=${DEFAULT_DELIMITER:-,}
fi
if [ -z "$QUOTE" ]; then
    QUOTE=${DEFAULT_QUOTE:-OFF}
fi

mkdir -p "$SPOOL_PATH"
SPOOL_FILE="$SPOOL_PATH/${SPOOL_FILE_NAME:-$FILE_NAME.csv}"
CONNECTION_STRING=$($PRECISION100_BIN_FOLDER/get-connection-string.sh "$CONNECTION_NAME")

$PRECISION100_BIN_FOLDER/audit.sh  $0 "PRE-SPOOL" "$CONTAINER / $FILE_NAME" "SPOOL" $0 "START"

$PRECISION100_OPERATORS_FOLDER/spool/bin/spool.sh $FILE_NAME $DELIMITER $QUOTE $SPOOL_FILE $CONNECTION_STRING

$PRECISION100_BIN_FOLDER/audit.sh  $0 "POST-SPOOL" "$CONTAINER / $FILE_NAME" "SPOOL" $0 "END"

echo "        END SPOOL ADAPTOR $FILE_NAME"
