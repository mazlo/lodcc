import argparse
import logging as log
import os
import re

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - merge edgelists in directory' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )

    log.basicConfig(
            level = log.INFO, 
            format = '[%(asctime)s] - %(levelname)-8s : %(message)s', )

    args = vars( parser.parse_args() )
    paths = args['paths']

    for path in paths:

        if not os.path.isdir( path ):
            log.error( '%s is not a directory', path )
            continue

        if re.search( '/$', path ):
            path = path[0:-1]

        log.info( 'handling %s', path )
        os.popen( './lodcc_merge_edgelists.sh %s' % path )

    log.info( 'done' )