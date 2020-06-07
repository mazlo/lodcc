import argparse
import logging as log
import os
import pickle
import re
import sys

from constants.edgelist import SUPPORTED_FORMATS
from graph.building.edgelist import iedgelist_edgelist

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc iedgelist' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--format', '-f', required = False, type = str, default = 'nt', help = '' )
    parser.add_argument( '--pickle', '-d', action = 'store_true', help = '' )
 
    log.basicConfig( level = log.INFO, 
                     format = '[%(asctime)s] - %(levelname)-8s : %(message)s', )

    args = vars( parser.parse_args() )
    paths   = args['paths']
    format_ = args['format']

    if not format_ in SUPPORTED_FORMATS:
        sys.exit()

    for path in paths:

        if os.path.isdir( path ):
            # if given path is directory get the .nt file there and transform

            if not re.search( '/$', path ):
                path = path+'/'

            for filename in os.listdir( path ):

                if not re.search( SUPPORTED_FORMATS[format_], filename ):
                    continue

                iedgelist_edgelist( path + filename, format_ )
        else:
            # if given path is a file, use it
            iedgelist_edgelist( path, format_ )
