#!/bin/bash
echo "Executing loader on-init-exec"

source $PRECISION100_FOLDER/conf/.operator.env.sh
mkdir -p $PRECISION100_OPERATOR_LOADER_INPUT_FOLDER
mkdir -p $PRECISION100_OPERATOR_LOADER_LOG_FOLDER
mkdir -p $PRECISION100_OPERATOR_LOADER_BAD_FOLDER
