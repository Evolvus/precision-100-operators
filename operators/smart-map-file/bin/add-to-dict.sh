#!/bin/bash

function usage() {
  echo "Usage $0 <new template file> <dictionary>"
  echo "e.g. $0 LRS.tsv dictionary.dat"
}

function cleanup() {
  rm -f sorted-input.txt.temp
  rm -f sorted-dictionary.txt.temp
  rm -f new-words.txt
  rm -f $2.temp
}

if [[ ( "$#" -ne 2 ) ]]; then
  usage
  exit 1
fi

cleanup

cat $1 | cut -d $'\t' -f 1 | tr '[:lower:]' '[:upper:]' | tr '()' ' ' | grep -o -E '\w+'  | sort -u | awk '{ print $0, $0 }' > sorted-input.txt.temp
cat $2 | cut -d ' ' -f 1 | awk '{ print $0, $0 }' | sort -u > sorted-dictionary.txt.temp
comm -23 sorted-input.txt.temp sorted-dictionary.txt.temp > new-words.txt
cat new-words.txt $2 > $2.temp 
sort -u $2.temp > $2

