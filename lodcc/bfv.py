import argparse
import logging as log
import os
import pickle
import re
import threading
import xxhash as xh

from ldicthash import parse_spo

def find_file( dataset ):
    """"""

    path = None

    if dataset['path_edgelist']:
        path = os.path.dirname( dataset['path_edgelist'] )
    elif dataset['path_graph_gt']:
        path = os.path.dirname( dataset['path_graph_gt'] )

    filename = '/'.join( [ path, dataset['name'] + '.nt' ] )

    if os.path.isfile( filename ):
        return filename

    log.error( 'Could not find nt-file in path %s', filename )
    return None

def save_hash( dataset, column, uri ):
    """"""

    sql = 'UPDATE stats_graph SET %(column)s=%(uri)s WHERE id=%(id)s'

    cur = conn.cursor()
    val_dict = { 'column': column + '_uri', 'uri': uri, 'id': dataset['id'] }
    #cur.execute( sql, { 'column': column, 'uri': uri, 'id': dataset['id'] } )
    #conn.commit()
    log.debug( sql % val_dict )
    cur.close()

def find_vertices( dataset, global_dict, sem=threading.Semaphore(1) ):
    """"""

    # can I?
    with sem:
        
        file = find_file( dataset )

        if not file:
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

        with open( file, 'r' ) as openedfile:
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
    parser.add_argument( '--vertices', '-n', nargs='*', required = True, help = '' )

    args = vars( parser.parse_args() )

    # configure log
    log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

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

    # load already found hashes
    pkl_file = open( 'found_hashes.pkl', 'rb')
    found_hashes = pickle.load( pkl_file )

    # setup threading
    sem = threading.Semaphore( args['processes'] )
    threads = []

    for dataset in datasets:

        t = threading.Thread( target = find_vertices, name = dataset['name'], args = ( dataset, found_hashes, sem ) )
        t.start()

        threads.append( t )

    for t in threads:
        t.join()
