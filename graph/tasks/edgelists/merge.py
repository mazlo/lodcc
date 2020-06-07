import argparse
import logging
import os
import re

from graph.building.edgelist import merge_edgelists

log = logging.getLogger( __name__ )

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - merge edgelists in directory' )
    parser.add_argument( '--from-file', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--rm-edgelists', '-re', action = "store_true", help = 'If given, the programm will remove single edgelist files after they have been appended to data.edgelist.csv' )

    args = vars( parser.parse_args() )
    dataset_names = args['from_file']

    merge_edgelists( dataset_names, args['rm_edgelists'] )

    log.info( 'done' )
