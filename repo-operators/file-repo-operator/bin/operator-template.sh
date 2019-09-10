#!/bin/bash


OPERATION_MODE=$1
REPO_ACTION=$2

case "$REPO_ACTION" in
  CHECKOUT) 
    cp -R $3/* $4
    ;;
  REFRESH)
    cp -R $3/* $4
    ;;
  BRANCH)
    ;;
  *)
    echo "Unknown file operation '$OPERATION_MODE' '$REPO_ACTION'" >&2
    ;;
esac
