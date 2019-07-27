#!/bin/bash
echo "Executing smart-map-file on-init-exec"

source $PRECISION100_OPERATORS_FOLDER/smart-map-file/conf/.operator.env.sh
mkdir -p $PRECISION100_OPERATOR_SMART_MAP_FILE_WORK_FOLDER;
