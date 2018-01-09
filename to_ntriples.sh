#!/bin/bash
# 
# given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.nt'
#

FILE_FORMAT="${1:-rdfxml}"
FILENAME=$2
FOLDER_DEST=$3 # e.g. dumps/dataset

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

# 1. bar.nt.tgz -> bar.nt
# 2. bar.tar.gz -> bar
# this will be the directory or filename
XMTYPE=`get_xmtype`
FILENAME_STRIPPED=`echo "$FILENAME" | sed "s/.$XMTYPE//"`

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

        echo "Extracting $FILENAME to $FOLDER_DEST"
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
        echo "Converting all files in folder $FILENAME_STRIPPED"
        for f in `find "$FILENAME_STRIPPED" -type f`; do
            rapper --input "$FILE_FORMAT" --output "ntriples" "$f" > "$f.nt"
        done
    fi

    # convert file
    if [[ -f "$FILENAME_STRIPPED" ]]; then
        echo "Converting $FILENAME_STRIPPED"
        rapper --input "$FILE_FORMAT" --output "ntriples" "$FILENAME_STRIPPED" > "$FILENAME_STRIPPED.nt"
    fi
}

do_oneliner()
{
    # check if extracted file is directory
    # if so, create one file from all the files there
    if [ -d "$FILENAME_STRIPPED" ]; then
        echo "Make oneliner"
        find "$FILENAME_STRIPPED" -name "*.nt" -type f -exec cat {} >> "$FILENAME_STRIPPED.tmp"  \; \
            && rm -rf "$FILENAME_STRIPPED" \
            && mv "$FILENAME_STRIPPED.tmp" "$FILENAME_STRIPPED.nt"
    fi
}

do_extract
do_convert
do_oneliner
