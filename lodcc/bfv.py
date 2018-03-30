import argparse
import logging as log
import os
import pickle
import psycopg2
import psycopg2.extras
import re
import threading
import xxhash as xh

from ldicthash import parse_spo

def find_path( dataset ):
    """"""

    if dataset['path_edgelist']:
        return os.path.dirname( dataset['path_edgelist'] )
    elif dataset['path_graph_gt']:
        return os.path.dirname( dataset['path_graph_gt'] )

    return None

def find_nt_files( path ):
    """"""

    return [ nt for nt in listdir( path ) if re.search( '\.nt$', nt ) ]

def save_hash( dataset, column, uri ):
    """"""

    sql = 'UPDATE stats_graph SET %(column)s=%(uri)s WHERE id=%(id)s'

    cur = conn.cursor()
    val_dict = { 'column': column + '_uri', 'uri': uri, 'id': dataset['id'] }
    #cur.execute( sql, { 'column': column, 'uri': uri, 'id': dataset['id'] } )
    #conn.commit()
    log.debug( sql % val_dict )
    cur.close()

def find_vertices( in_file, dataset, global_dict, sem=threading.Semaphore(1) ):
    """"""

    # can I?
    with sem:
        if not in_file:
            log.error( 'Exiting because of previrous errors' )
            return

        col_names = ['max_degree_vertex', 'max_pagerank_vertex']
        hashes_to_find = [ dataset[i] for i in col_names ]
        hashes_to_save = {}

        # check if already found
        for idx,h in enumerate(hashes_to_find):
            if h and h in global_dict:
                # e.g. save_hash( 'max_degree_vertex', 'http://mydomain.com/class#' )
                save_hash( dataset, col_names[idx], global_dict[h] )
                hashes_to_find.remove( h )

        if len( hashes_to_find ) == 0:
            return  # done

        with open( in_file, 'r' ) as openedfile:
            for line in openedfile:

                s,p,o = parse_spo( line, '.nt$' )

                uris = [s,o]
                sh = xh.xxh64( s ).hexdigest()
                oh = xh.xxh64( o ).hexdigest()
                
                for current_hash in hashes_to_find:
                    for idx,e in enumerate( [sh,oh] ):
                        if e == current_hash:
                            # found one, save it
                            save_hash( dataset, col_names[idx], uris[idx] )
                            hashes_to_find.remove( current_hash )
                            break

                # checked once, over?
                if len( hashes_to_find ) == 0:
                    break   # done

if __name__ == '__main__':

    # configure args parser
    parser = argparse.ArgumentParser( description = 'lodcc - find xxhash from original data' )
    parser.add_argument( '--use-datasets', '-du', nargs='*', required=True, help = '' )
    parser.add_argument( '--processes', '-dp', type=int, required=False, default=1, help = '' )
    parser.add_argument( '--vertices', '-n', nargs='*', required=False, help = '' )

    parser.add_argument( '--log-debug', action='store_true', help = '' )

    args = vars( parser.parse_args() )

    if args['log_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    # configure log
    log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    # read all properties in file into args-dict
    if os.path.isfile( 'db.properties' ):
        with open( 'db.properties', 'rt' ) as f:
            args = dict( ( key.replace( '.', '-' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) )
    else:
        log.error( 'Please verify your settings in db.properties (file exists?)' )
        sys.exit()

    z = vars( parser.parse_args() ).copy()
    z.update( args )
    args = z

    # connect to an existing database
    conn = psycopg2.connect( host=args['db-host'], dbname=args['db-dbname'], user=args['db-user'], password=args['db-password'] )
    conn.set_session( autocommit=True )

    # check database connection
    try:
        cur = conn.cursor()
        cur.execute( "SELECT 1;" )
        result = cur.fetchall()
        cur.close()

        log.debug( 'Database ready to query execution' )
    except:
        log.error( 'Database not ready for query execution. %s', sys.exc_info()[0] )
        raise 

    # respect --use-datasets argument
    if not args['use_datasets'] or len( args['use_datasets'] ) == 0:
        log.error( '--use-datasets not provided' )
        sys.exit()

    names_query = '( ' + ' OR '.join( 'name = %s' for ds in args['use_datasets'] ) + ' )'
    names = tuple( args['use_datasets'] )

    sql = 'SELECT id,name,max_degree_vertex,max_pagerank_vertex,path_edgelist,path_graph_gt FROM stats_graph WHERE '+ names_query +' AND (max_degree_vertex IS NOT NULL OR max_pagerank_vertex IS NOT NULL) ORDER BY id'

    # get datasets from database
    cur = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
    cur.execute( sql, names )

    if cur.rowcount == 0:
        log.warning( '--use-datasets specified but empty result set' )
        sys.exit()

    datasets = cur.fetchall()

    if os.path.isfile( 'found_hashes.pkl' ):
        # load already found hashes
        pkl_file = open( 'found_hashes.pkl', 'rb')
        found_hashes = pickle.load( pkl_file )
    else:
        found_hashes = {}

    # setup threading
    sem = threading.Semaphore( args['processes'] )
    threads = []

    for dataset in datasets:

        path = find_path( dataset )
        files = find_nt_files( path )

        for file in files:
            t = threading.Thread( target = find_vertices, name = dataset['name'], args = ( file, dataset, found_hashes, sem ) )
            t.start()
            t.join()

            threads.append( t )
