#!/bin/bash
OPERATOR_NAME=map-file
echo "Installing $OPERATOR_NAME operator"

# We need to install the masking procedures over here..for now lets leave it.

CONNECTION_NAME=$PRECISION100_PROJECT_DEFAULT_CONNECTION
CONNECTION_STRING=$($PRECISION100_BIN_FOLDER/get-connection-string.sh $CONNECTION_NAME)

sqlplus -s /nolog << EOF
CONNECT $CONNECTION_STRING
@$PRECISION100_OPERATORS_FOLDER/masking-mapper/sql/setup.sql
EOF
