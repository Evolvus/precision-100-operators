CONTAINER=$1
INDEX=$(echo $2 | cut -d ',' -f 1)
FILE_NAME=$(echo $2 | cut -d ',' -f 2)
FILE_TYPE=$(echo $2 | cut -d ',' -f 3)
CONNECTION_NAME=$(echo $2 | cut -d ',' -f 4)


if test "$PRECISION100_RUNTIME_SIMULATION_MODE" = "TRUE"; then
   echo "        START SQL_LOADER ADAPTOR $FILE_NAME"
   sleep $PRECISION100_RUNTIME_SIMULATION_SLEEP;
   echo "        END SQL_LOADER ADAPTOR $FILE_NAME"
   exit;
fi

echo "        START SQL_LOADER ADAPTOR $FILE_NAME"

source $PRECISION100_OPERATORS_FOLDER/loader/conf/.operator.env.sh
LOADER_FILE_NAME=${FILE_NAME%.*}
CONTROL_FILE="$PRECISION100_EXECUTION_CONTAINER_FOLDER/$CONTAINER/$LOADER_FILE_NAME.ctl"
DATA_FILE="$PRECISION100_OPERATOR_LOADER_INPUT_FOLDER/$LOADER_FILE_NAME.dat"
LOG_FILE="$PRECISION100_OPERATOR_LOADER_LOG_FOLDER/$LOADER_FILE_NAME.log"
BAD_FILE="$PRECISION100_OPERATOR_LOADER_BAD_FOLDER/$LOADER_FILE_NAME.bad"


DIRECT=${DEFAULT_DIRECT:-TRUE}
ERRORS=${DEFAULT_ERRORS:-1000000}
BINDSIZE=${DEFAULT_BINDSIZE:-5048576}
MULTITHREADING=${DEFAULT_MULTITHREADING:-TRUE}

CONNECTION_STRING=$($PRECISION100_BIN_FOLDER/get-connection-string.sh "$CONNECTION_NAME")

$PRECISION100_BIN_FOLDER/audit.sh  $0 "PRE-LOADER" "$CONTAINER / $FILE_NAME" "LOADER" $0 "START"

sqlldr control=$CONTROL_FILE data=$DATA_FILE log=$LOG_FILE bad=$BAD_FILE direct=$DIRECT errors=$ERRORS bindsize=$BINDSIZE multithreading=$MULTITHREADING << LOADER 
$CONNECTION_STRING
LOADER

$PRECISION100_BIN_FOLDER/audit.sh  $0 "POST-LOADER" "$CONTAINER / $FILE_NAME" "LOADER" $0 "END"

echo "        END SQL_LOADER ADAPTOR $FILE_NAME"
