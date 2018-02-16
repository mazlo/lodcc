#!/bin/bash

FOLDER_SRC=`pwd`
# arg: folder, e.g. dumps/foo-ds
FOLDER_DEST=$1
# arg: filename, e.g. bar.nt.tgz
FILENAME=$2
# arg: extension, e.g. .tgz
MTYPE=$3

# cd and extract
cd $FOLDER_DEST
dtrx --one rename --overwrite $FILENAME

# e.g. bar.nt.tgz becomes bar.nt
FILE_STRIPPED=`echo "$FILENAME" | sed "s/$MTYPE//"`

# check if extracted file is directory
# if so, create one file from all the files there
if [ -d "$FILE_STRIPPED" ]; then
  find "$FILE_STRIPPED" -type f -exec cat {} >> "$FILE_STRIPPED.tmp"  \; \
  && rm -rf "$FILE_STRIPPED" \
  && mv "$FILE_STRIPPED.tmp" "$FILE_STRIPPED"
fi

# cd back to root folder
cd $FOLDER_SRC
