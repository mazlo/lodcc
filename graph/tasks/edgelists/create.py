import argparse
import logging
import xxhash as xh
import os
import re
import threading

from graph.building.edgelist import create_edgelist, xxhash_csv

log = logging.getLogger( __name__ )

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc task - Creates edgelists for graph instantiation from RDF datasets of N-Triples format. This is an internal helper function. If you do not know what you are doing, use graph.tasks.prepare instead.' )
    parser.add_argument( '--from-file', '-ffl', nargs='+', required = True, help = 'List of directory names where to find the RDF dataset. Example value: dumps/oecd-linked-data/' )
    parser.add_argument( '--format', '-f', required=False, type=str, default='nt', help='Obsolete parameter. Possible values are csv or nt. Default: nt.' )

    args = vars( parser.parse_args() )
    paths = args['from_file']
    sem = threading.Semaphore( 8 )
    threads = []

    if args['format'] == 'nt':
        method = create_edgelist
    else:
        method = xxhash_csv
    
    for path in paths:
        if os.path.isdir( path ):
            if not re.search( '/$', path ):
                path = path+'/'

            for filename in os.listdir( path ):
                if args['format'] == 'csv':
                    if not re.search( '.csv$', filename ):
                        continue
                    if 'edgelist' in filename:
                        continue

                t = threading.Thread( target = method, name = filename, args = ( path + filename, sem ) )
                t.start()

                threads.append( t )

            for t in threads:
                t.join()
        else:
            method( path )
