#!/bin/bash
#
# This script takes a file with ntriple format as input. It converts this file
# into a edge-list structure, i.e. "node_src node_trg { edge }".
# Given a file with name 'foo.bar' or 'foo.bar.nt', this process will write the
# data into a file named: 'foo.bar.csv'
#

FPATH="$1"
# from PATH
FILENAME=`echo ${FPATH##*/}`

get_xmtype()
{
    for mtype in 'tar.gz' 'tar.xz' 'tgz' 'gz' 'zip' 'bz2' 'tar'; do
        # ${"foo.bar.tar.gz"%*.tar.gz} returns "foo.bar"
        # ${"foo.bar.bz2"%*.gz} returns "foo.bar.bz2"
        if [[ "${FILENAME%*.$mtype}" != "$FILENAME" ]]; then
            echo $mtype
            return
        fi
    done
}

file_exists() 
{
    if [ ! -f "$FPATH_OUTPUT" ]; then
        return 1 # exit failure
    fi

    SIZE=`ls -s "$FPATH_OUTPUT" | cut -d ' ' -f1`
    if [[ $NO_CACHE = false && $SIZE > 1000 ]]; then
        return 0 # exit success
    fi

    return 1 # exit failure
}

# 1. dumps/foo/bar.nt.tgz -> dumps/foo/bar.nt
# 2. dumps/foo/bar.tar.gz -> dumps/foo/bar
# this will be the directory or filename
XMTYPE=`get_xmtype`

FPATH_STRIPPED=`echo ${FPATH%*.$XMTYPE}`
FPATH_OUTPUT="$FPATH_STRIPPED.csv"

NO_CACHE=${2:-false}
INPUT_EXT=${3:-"nt"}

# we're reading from a file with $INPUT_EXT, so
# add extension if the file does not have it already
if [[ "${FPATH_STRIPPED%*.$INPUT_EXT}" == "$FPATH_STRIPPED" ]]; then
    FPATH_STRIPPED="$FPATH_STRIPPED.$INPUT_EXT"
fi

if file_exists; then
    exit 0 # exit success
fi

# paste the content | add a . at the end | as long as there are lines, rewrite their position
cat "$FPATH_STRIPPED" | sed 's#.$##' | while read -r s p o; do echo "$s $o {'edge':'$p'}"; done > "$FPATH_OUTPUT"

