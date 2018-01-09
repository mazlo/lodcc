#!/bin/bash
# 
# given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.nt'
#

FILE_FORMAT="${1:-rdfxml}"
FILENAME=$2
FOLDER_DEST=$3 # e.g. dumps/dataset
XMTYPE=$4 # e.g. .tar.gz

# 1. bar.nt.tgz -> bar.nt
# 2. bar.tar.gz -> bar
# this will be the directory name
FILENAME_STRIPPED=`echo "$FILENAME" | sed "s/$XMTYPE//"`

do_extract()
{
    # 'tar.gz', 'tar.xz', 'tgz', 'gz', 'zip', 'bz2', 'tar'
    if  [[ $XMTYPE == '*.tar.gz' ]] || 
        [[ $XMTYPE == '*.tar.xz']] || 
        [[ $XMTYPE == '*.tgz' ]] || 
        [[ $XMTYPE == '*.gz' ]] || 
        [[ $XMTYPE == '*.zip' ]] ||
        [[ $XMTYPE == '*.bz2' ]] || 
        [[ $XMTYPE == '*.tar' ]]; then

        FOLDER_SRC=`pwd`
        cd $FOLDER_DEST
        dtrx --one rename --overwrite $FILENAME
        cd $FOLDER_SRC
    fi
}

do_convert()
{
    # convert all files in directory
    if [[ -d "$FILENAME_STRIPPED" ]]; then
        for f in $FOLDER_DEST; do
            rapper --ignore-errors --quiet --input "$FILE_FORMAT" --output "ntriples" "$f" > "$f.nt"
        done
    fi

    # convert file
    if [[ -f "$FILENAME_STRIPPED" ]]; then
        rapper --ignore-errors --quiet --input "$FILE_FORMAT" --output "ntriples" "$FILENAME" > "$FILENAME.nt"
    fi
}

do_oneliner()
{
    # check if extracted file is directory
    # if so, create one file from all the files there
    if [ -d "$FILENAME_STRIPPED" ]; then
      find "$FILENAME_STRIPPED" -type f -exec cat {} >> "$FILENAME_STRIPPED.tmp"  \; \
      && rm -rf "$FILENAME_STRIPPED" \
      && mv "$FILENAME_STRIPPED.tmp" "$FILENAME_STRIPPED"
    fi
}

do_extract
do_convert
do_oneliner
