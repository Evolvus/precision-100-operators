OPERATOR_NAME=smart-loader
echo "Installing $OPERATOR_NAME operator"

source $PRECISION100_OPERATORS_FOLDER/smart-loader/conf/.operators.env.sh

mkdir -p "$SQLLDR_INPUT"
mkdir -p "$SQLLDR_LOG"
mkdir -p "$SQLLDR_BAD"
mkdir -p "$SMART_SQLLDR_CTL_FOLDER";
