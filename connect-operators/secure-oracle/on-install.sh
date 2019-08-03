#!/bin/bash

CONF_FILE=$PRECISION100_CONNECT_OPERATORS_FOLDER/secure-oracle/conf/.operator.env.sh  
cat > "$CONF_FILE" << EOL
CONFIG_SEPARATOR=',' 

NAME_INDEX=1
TYPE_INDEX=2
USERNAME_INDEX=3
SID_INDEX=4
EOL

openssl enc -aes-256-cbc -iter 10 -k $(date +%s) -P | tr -d ' ' >> "$CONF_FILE"
