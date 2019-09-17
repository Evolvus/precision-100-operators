#!/bin/bash
echo "Executing spool on-init-exec"

source $PRECISION100_OPERATORS_FOLDER/t24-parameter-xml-csv/conf/.operator.env.sh
mkdir -p $PRECISION100_OPERATOR_T24_PARAMETER_XML_CSV_INPUT_FOLDER
mkdir -p $PRECISION100_OPERATOR_T24_PARAMETER_XML_CSV_OUTPUT_FOLDER
