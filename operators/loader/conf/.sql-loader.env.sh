export ORACLE_HOME=/home/dell/pcs100/new-precision/precision-100-framework
export LD_LIBRARY_PATH="$ORACLE_HOME"/lib
export PATH="$ORACLE_HOME/bin:$PATH"
export TNS_ADMIN="$ORACLE_HOME/lib/network/admin"
export NLS_LANG=".UTF8"

export SQLLDR_INPUT=$PRECISION100_EXECUTION_FOLDER/sqlldr_input
export SQLLDR_LOG=$PRECISION100_EXECUTION_FOLDER/sqlldr_log
export SQLLDR_BAD=$PRECISION100_EXECUTION_FOLDER/sqlldr_bad
export DEFAULT_DIRECT=TRUE
export DEFAULT_ERRORS=1000000
export DEFAULT_BINDSIZE=5048576
export DEFAULT_MULTITHREADING=TRUE
export DEFAULT_LINES_TO_SKIP=1
export DEFAULT_CHARSET="UTF8"
