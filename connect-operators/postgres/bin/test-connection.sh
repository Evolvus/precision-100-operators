#/bin/bash

function usage() {
  echo "Usage: $0 <connection name> [test sql]"
  echo "e.g. $0 PRECISION100_CONNECTION"
  echo "e.g. $0 PRECISION100_CONNECTION 'SELECT SYSDATE FROM DUAL'"
}

if [[ ( "$#" -gt 2 ) || ( "$#" -lt 1 ) ]]; then
  usage
  exit 1
fi

if [ ! -f ./conf/.project.env.sh ]; then
   echo "Misconfigured installation - missing files in conf directory"
   exit 10
fi
source ./conf/.project.env.sh

if [ -z "$PRECISION100_FOLDER" ] || [ ! -f $PRECISION100_FOLDER/conf/.env.sh ]; then
   echo "Misconfigured installation - Invalid Precision100 installation"
   exit 10
fi
source $PRECISION100_FOLDER/conf/.env.sh

CONNECTION_NAME=$1
TEST_SQL=${2:-"SELECT SYSDATE FROM DUAL"}
CONNECTION_STRING=$($PRECISION100_BIN_FOLDER/get-connection-string.sh "$CONNECTION_NAME")

sqlplus -s /nolog  <<EOF 
connect $CONNECTION_STRING
$TEST_SQL
exit;
EOF
