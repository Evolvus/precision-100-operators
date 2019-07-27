#!/bin/bash
echo "Executing length-validator on-init-exec"

source $PRECISION100_OPERATORS_FOLDER/length-validator/conf/.operator.env.sh
mkdir -p $PRECISION100_OPERATOR_LENGTH_VALIDATOR_WORK_FOLDER;
