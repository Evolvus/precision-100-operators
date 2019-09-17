CONTAINER=$1
INDEX=$(echo $2 | cut -d ',' -f 1)
FILE_NAME=$(echo $2 | cut -d ',' -f 2)
FILE_TYPE=$(echo $2 | cut -d ',' -f 3)
DAT_FILE_NAME=$(echo $2 | cut -d ',' -f 4)
OPERATION=$(echo $2 | cut -d ',' -f 5)
DATA_FILE_SEPARATOR=$(echo $2 | cut -d ',' -f 6)
CONNECTION_NAME=$(echo $2 | cut -d ',' -f 7)

LOADER_FILE_NAME=${FILE_NAME}

if test "$PRECISION100_RUNTIME_SIMULATION_MODE" = "TRUE"; then
   echo "        START SMART_LOADER ADAPTOR $FILE_NAME"
   sleep $PRECISION100_RUNTIME_SIMULATION_SLEEP;
   echo "        END SMART_LOADER ADAPTOR $FILE_NAME"
   exit;
fi

echo "        START SMART_LOADER ADAPTOR $FILE_NAME"

source $PRECISION100_OPERATORS_FOLDER/smart-loader/conf/.operator.env.sh

if [[ -z "$DATA_FILE_SEPARATOR" ]]; then
   DATA_FILE_SEPARATOR=${DEFAULT_DATA_FILE_SEPARATOR:-,}
fi

if [[ -z "$OPERATION" ]]; then
   OPERATION=${OPERATION:-TRUNCATE}
fi

CONTROL_FILE="$PRECISION100_OPERATOR_SMART_LOADER_CTL_FOLDER/$LOADER_FILE_NAME.ctl"
DATA_FILE="$PRECISION100_OPERATOR_SMART_LOADER_INPUT_FOLDER/${DAT_FILE_NAME:-$FILE_NAME.dat}"
LOG_FILE="$PRECISION100_OPERATOR_SMART_LOADER_LOG_FOLDER/$LOADER_FILE_NAME.log"
BAD_FILE="$PRECISION100_OPERATOR_SMART_LOADER_BAD_FOLDER/$LOADER_FILE_NAME.bad"

CONNECTION_STRING=$($PRECISION100_BIN_FOLDER/get-connection-string.sh "$CONNECTION_NAME")
$PRECISION100_OPERATORS_FOLDER/smart-loader/bin/control-file-generator.sh $LOADER_FILE_NAME $OPERATION $DATA_FILE_SEPARATOR $CONNECTION_STRING > $CONTROL_FILE 

DIRECT=${DEFAULT_DIRECT:-TRUE}
ERRORS=${DEFAULT_ERRORS:-1000000}
BINDSIZE=${DEFAULT_BINDSIZE:-5048576}
MULTITHREADING=${DEFAULT_MULTITHREADING:-TRUE}
LINES_TO_SKIP=${DEFAULT_LINES_TO_SKIP:-1}



$PRECISION100_BIN_FOLDER/audit.sh  $0 "PRE-SMART-LOADER" "$CONTAINER / $FILE_NAME" "SMART-LOADER" $0 "START"

sqlldr control=$CONTROL_FILE data=$DATA_FILE log=$LOG_FILE bad=$BAD_FILE direct=$DIRECT errors=$ERRORS bindsize=$BINDSIZE multithreading=$MULTITHREADING  skip=${LINES_TO_SKIP} << LOADER 
$CONNECTION_STRING
LOADER

$PRECISION100_BIN_FOLDER/audit.sh  $0 "POST-SMART-LOADER" "$CONTAINER / $FILE_NAME" "SMART-LOADER" $0 "END"

echo "        END SMART_LOADER ADAPTOR $FILE_NAME"
