#/bin/bash

function usage() {
  echo "Usage: $0 <connection name> <password>"
  echo "e.g. $0 PRECISION100_CONNECTION Welcome123"
}

if [[ ( "$#" -ne 2 ) ]]; then
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

source $PRECISION100_CONNECT_OPERATORS_FOLDER/secure-oracle/conf/.operator.env.sh

echo $2 | openssl enc -e -aes-256-cbc -out ./conf/"$1".enc -base64 -K $key -iv $iv
