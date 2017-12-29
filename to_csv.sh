#!/bin/bash
FILE="$1"

if [[ ! $FILE == *.nt ]]; then
    FILE="$1.nt"
fi

cat "$FILE" | sed 's#.$##' | while read -r s p o; do echo "$s $o {'edge':'$p'}"; done > "$1.csv"
