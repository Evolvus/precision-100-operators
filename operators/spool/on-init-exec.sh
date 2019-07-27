#!/bin/bash
echo "Executing spool on-init-exec"

source $PRECISION100_OPERATORS_FOLDER/spool/conf/.operator.env.sh
mkdir -p $PRECISION100_OPERATOR_SPOOL_FOLDER
