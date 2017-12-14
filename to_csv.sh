#!/bin/bash
cat "$1.nt" | sed 's#.$##' | while read -r s p o; do echo "$s $o {'edge':'$p'}"; done > "$1.csv"
