#!/bin/bash
echo "Executing masking-mapper on-init-exec"

source $PRECISION100_OPERATORS_FOLDER/masking-mapper/conf/.operator.env.sh
mkdir -p $MASKING_MAPPER_WORK_FOLDER;
