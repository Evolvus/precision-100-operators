#!/bin/bash


OPERATION_MODE=$1
REPO_ACTION=$2

case "$REPO_ACTION" in
  CHECKOUT) 
    git clone $3 $4
    ;;
  REFRESH)
    git pull
    ;;
  BRANCH)
    git branch "$3"
    git checkout "$3"
    git push -u origin "$3"
    ;;
  *)
    echo "Unknown git action '$OPERATION_MODE' '$REPO_ACTION'" 1>&2
    ;;
esac
