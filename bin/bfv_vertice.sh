#!/bin/bash
#
# this script finds a matching uri to a given hashed vertice value
#
# ATTENTION
# Be sure that your files have the exact same number of lines 
# with the exact same ordering and corresponding values.
# 

VERTICE=$1
EDGELIST=$2
NT=$3

MATCHED_LINE=`egrep -n $VERTICE -m 1 $EDGELIST`
LNUMBER=`echo "$MATCHED_LINE" | cut -d ':' -f1 `
RES=`sed -n "$LNUMBER"p $NT`

corresponding_uri()
{
    idx=1
    for m in `echo $MATCHED_LINE | cut -d ':' -f2 -f3`; do
        if [[ $VERTICE == $m ]]; then
            echo $RES | while read -r s p o; do
                if [[ $idx == 1 ]]; then 
                    echo $s
                    return
                elif [[ $idx == 2 ]]; then
                    echo $o
                    return
                fi
            done
        fi
        let "idx += 1"
    done    
}

corresponding_uri

