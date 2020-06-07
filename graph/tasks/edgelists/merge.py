import argparse
import logging
import os
import re

from graph.building.edgelist import merge_edgelists

log = logging.getLogger( __name__ )

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc task - Merges edgelists created by individual RDF dataset files into a combined data.edgelist.csv file. This is an internal helper function. If you do not know what you are doing, use graph.tasks.prepare instead.' )
    parser.add_argument( '--from-file', '-ffl', nargs='+', required = True, help = '' )
    parser.add_argument( '--rm-edgelists', '-re', action = "store_true", help = 'Remove intermediate edgelists, obtained from individual files, after creating a combined data.edgelist.csv file. Default False.' )

    args = vars( parser.parse_args() )
    dataset_names = args['from_file']

    merge_edgelists( dataset_names, args['rm_edgelists'] )

    log.info( 'done' )
