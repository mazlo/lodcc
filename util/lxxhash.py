import argparse
import xxhash as xh
import os
import re
import threading

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc xxhash' )
    parser.add_argument( '--path', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--format', '-f', required=False, type=str, default='nt', help='' )

    args = vars( parser.parse_args() )
    paths = args['path']
    sem = threading.Semaphore( 8 )
    threads = []

    if args['format'] == 'nt':
        method = xxhash_nt
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
