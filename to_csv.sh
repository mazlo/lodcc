#!/bin/bash
#
# given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.csv'
#

FILE="$1"

# add .nt extension if the file does not have it already
if [[ ! $FILE == *.nt ]]; then
    FILE="$1.nt"
fi

# paste the content | add a . at the end | as long as there are lines, rewrite their position
cat "$FILE" | sed 's#.$##' | while read -r s p o; do echo "$s $o {'edge':'$p'}"; done > "$1.csv"
