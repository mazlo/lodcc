import argparse
import logging as log
import os
import pickle
try:
    import psycopg2
    import psycopg2.extras
except:
    print 'psycogp2 could not be found'
import re
import xxhash as xh

from ldicthash import parse_spo

def find_nt_files( path ):
    """"""

    return [ nt for nt in os.listdir( path ) if re.search( '\.nt$', nt ) ]

def bf_necessary( vertices_map ):
    """"""

    if len( vertices_map ) == 0:
        return {}

    # vertices_map = { 'v1': ( 'value', False ) }
    # False, in case the value still has to be dereferenced

    return dict( { v[0]:v for k,v in vertices_map.items() if type(v) == tuple and not v[1] } )

def find_vertices( in_file, dataset, vertices_map ):
    """"""

    if not in_file:
        log.error( 'Exiting because of previrous errors' )
        return vertices_map

    left_to_bf = bf_necessary( vertices_map )
    if len( left_to_bf ) == 0:
        return vertices_map

    with open( in_file, 'r' ) as openedfile:
        for line in openedfile:

            s,p,o = parse_spo( line, '.nt$' )

            sh = xh.xxh64( s ).hexdigest()
            ph = xh.xxh64( p ).hexdigest()
            oh = xh.xxh64( o ).hexdigest()

            if sh in left_to_bf:
                vertices_map[sh] = (s,True)

            if ph in left_to_bf:
                vertices_map[ph] = (p,True)

            if oh in left_to_bf:
                vertices_map[oh] = (o,True)

            # over?
            left_to_bf = bf_necessary( vertices_map )
            if len( left_to_bf ) == 0:
                break   # done

        return vertices_map

def job_find_vertices( dataset, vertices ):
    """"""

    if not os.path.isdir( dataset ):
        log.error( 'Dataset %s is not a directory.' % ( dataset ) )
        return

    files = find_nt_files( dataset )

    if len( files ) == 0:
        log.warning( 'No nt-file found for dataset %s', dataset  )
        return

    if type( vertices ) == list:
        # before: vertices = [ 'v1', 'v2', ... ]
        # after: vertices = { 'v1': ('v1', False), 'v2': ('v2', False), ... }
        vertices = dict( { v:(v,False) for v in vertices } )

    log.debug( 'Scanning %s files (in %s)' % ( len(files), files ) )
    for file in files:
        vertices = find_vertices( '/'.join( [dataset,file] ), dataset, vertices )

        # over?
        left_to_bf = bf_necessary( vertices )
        if len( left_to_bf ) == 0:
            break   # done

    # before: vertices = { 'v1': ('v1', False), 'v2': ('v2', False), ... }
    # after: vertices = { 'v1': 'v1', 'v2': 'v2', ... }
    return dict( { k:v[0] for k,v in vertices.items() } )

if __name__ == '__main__':

    # configure args parser
    parser = argparse.ArgumentParser( description = 'lodcc - find xxhash from original data' )
    parser.add_argument( '--from-file', '-ffl', action = "append", help = '', nargs = '*')
    parser.add_argument( '--processes', '-dp', type=int, required=False, default=1, help = '' )
    parser.add_argument( '--vertices', '-n', nargs='*', required=True, help = '' )

    parser.add_argument( '--log-debug', action='store_true', help = '' )

    args = vars( parser.parse_args() )

    if args['log_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    # configure log
    log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    z = vars( parser.parse_args() ).copy()
    z.update( args )
    args = z

    datasets = args['from_file']        # argparse returns [[..], [..],..]
    datasets = map( lambda d: 'dumps/%s' % d[0], datasets )

    for dataset in datasets:
        print( job_find_vertices( dataset, args['vertices'] ) )
