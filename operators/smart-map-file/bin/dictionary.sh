#!/bin/bash

#
#Script to get unique words from a file
#cat LAM.tsv | cut -d $'\t' -f 1 | tr '[:lower:]' '[:upper:]' | tr -s ')' ' ' | tr -s '(' ' ' | grep -o -E '\w+'  | sort -u | awk '{ print $0, $0 }'
#

cat $PRECISION100_CONTAINERS_FOLDER/$CONTAINERS/$FILENAME.tsv | cut -d $'\t' -f 1 | tr '[:lower:]' '[:upper:]' | tr -s ')' ' ' | tr -s '(' ' ' | tr -s '[:punct:]' ' ' | sed 's/[[:blank:]]*$//' > fields1.txt
awk -f dict.awk dict1.dat fields1.txt | tr ' ' '_'
