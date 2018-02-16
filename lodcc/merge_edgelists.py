import argparse
import logging as log
import os
import re

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - merge edgelists in directory' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--rm-edgelists', '-re', action = "store_true", help = 'If given, the programm will remove single edgelist files after they have been appended to data.edgelist.csv' )

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
        os.popen( './bin/merge_edgelists.sh %s %s' % (path,args['rm_edgelists']) )

    log.info( 'done' )
