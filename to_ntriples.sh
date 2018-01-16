#!/bin/bash
# 
# This script takes an rdf format and a file as input. It converts the file
# from the given format into ntriples, while it also extracts the file if
# necessary. If the compressed file is an archive containing more than one
# file, all files will be converted and merged.
# 
# Given a file with name 'foo.bar' or 'foo.bar.tar.gz', this process will 
# write the data into a file named: 'foo.bar.nt'.
#

FILE_FORMAT="${1:-rdfxml}"
FPATH="$2" # e.g. dumps/foo/bar.gz

# from PATH
FILENAME=`echo ${FPATH##*/}`
FOLDER_DEST=`echo ${FPATH%/*}`

file_exists()
{
    if [ ! -f "$FPATH_STRIPPED.nt" ]; then
        return 1 # exit failure
    fi

    SIZE=`ls -s "$FPATH_STRIPPED.nt" | cut -d ' ' -f1`
    if [[ $NO_CACHE = false && $SIZE > 1000 ]]; then
        return 0 # exit success
    fi
    
    return 1 # exit failure
}

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

do_extract()
{
    # 'tar.gz', 'tar.xz', 'tgz', 'gz', 'zip', 'bz2', 'tar'
    if  [[ $XMTYPE == 'tar.gz' ]] || 
        [[ $XMTYPE == 'tar.xz' ]] || 
        [[ $XMTYPE == 'tgz' ]] || 
        [[ $XMTYPE == 'gz' ]] || 
        [[ $XMTYPE == 'zip' ]] ||
        [[ $XMTYPE == 'bz2' ]] || 
        [[ $XMTYPE == 'tar' ]]; then

        #echo "Extracting $FILENAME to $FOLDER_DEST"
        FOLDER_SRC=`pwd`
        cd $FOLDER_DEST
        dtrx --one rename --overwrite $FILENAME
        cd $FOLDER_SRC
    fi
}

do_convert()
{
    # ignore ntriple INPUT format
    if [[ $FILE_FORMAT == 'ntriples' ]]; then
        return 0 # return success
    fi

    # convert all files in directory
    if [[ -d "$FPATH_STRIPPED" ]]; then
        #echo "Converting all files in folder $FPATH_STRIPPED"
        for f in `find "$FPATH_STRIPPED" -type f`; do
            rapper --input "$FILE_FORMAT" --output "ntriples" "$f" > "$f.nt"
        done
    fi

    # convert file
    if [[ -f "$FPATH_STRIPPED" ]]; then
        #echo "Converting $FPATH_STRIPPED"
        rapper --input "$FILE_FORMAT" --output "ntriples" "$FPATH_STRIPPED" > "$FPATH_STRIPPED.nt"
    fi
}

do_oneliner()
{
    # check if extracted file is directory
    # if so, create one file from all the files there
    if [ -d "$FPATH_STRIPPED" ]; then
        #echo "Make oneliner"
        find "$FPATH_STRIPPED" -name "*.nt" -type f -exec cat {} >> "$FPATH_STRIPPED.tmp"  \; \
            && rm -rf "$FPATH_STRIPPED" \
            && mv "$FPATH_STRIPPED.tmp" "$FPATH_STRIPPED.nt"
    fi
}

# 1. dumps/foo/bar.nt.tgz -> dumps/foo/bar.nt
# 2. dumps/foo/bar.tar.gz -> dumps/foo/bar
# this will be the directory or filename
XMTYPE=`get_xmtype`
FPATH_STRIPPED=`echo ${FPATH%*.$XMTYPE}`

NO_CACHE=${3:-false}

if file_exists; then
    exit 0 # exit success
fi

do_extract
do_convert
do_oneliner
