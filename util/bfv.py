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

    return [ nt for nt in os.listdir( path ) if re.search( '\.nt$', nt ) ]

def save_hash( dataset, column, uri ):
    """"""

    sql = 'UPDATE stats_graph SET '+ column +'_uri=%(uri)s WHERE id=%(id)s'
    val_dict = { 'uri': uri, 'id': dataset['id'] }

    cur = conn.cursor()
    cur.execute( sql, val_dict )
    conn.commit()
    cur.close()
    
    log.debug( sql % val_dict )

def get_hashes_to_find( dataset, col_names ):
    """"""

    hashes_to_find = {} 
    for name in col_names:
        hashv = dataset[name]
        if not hashv:
            continue

        if hashv in hashes_to_find:
            hashes_to_find[hashv].append( name )
        else:
            hashes_to_find[hashv] = [name]

    return hashes_to_find

def find_vertices( in_file, dataset, hashes_to_find ):
    """"""

    if not in_file:
        log.error( 'Exiting because of previrous errors' )
        return

    with open( in_file, 'r' ) as openedfile:
        for line in openedfile:

            s,_,o = parse_spo( line, '.nt$' )

            sh = xh.xxh64( s ).hexdigest()
            oh = xh.xxh64( o ).hexdigest()
            
            if sh in hashes_to_find:
                cols = hashes_to_find[sh]
                for col in cols:
                    save_hash( dataset, col, s )
                
                del hashes_to_find[sh]

            if oh in hashes_to_find:
                cols = hashes_to_find[oh]
                for col in cols:
                    save_hash( dataset, col, o )
                
                del hashes_to_find[oh]

            # checked, over?
            if len( hashes_to_find ) == 0:
                break   # done

def job_find_vertices( dataset, sem ):
    """"""

    # can I?
    with sem:
        path = find_path( dataset )
        files = find_nt_files( path )

        if len( files ) == 0:
            log.warning( 'No nt-file found for dataset %s', dataset['name']  )
            return

        col_names = ['max_degree_vertex', 'max_pagerank_vertex', 'max_in_degree_vertex', 'max_out_degree_vertex'] #,'pseudo_diameter_src_vertex', 'pseudo_diameter_trg_vertex']
        hashes_to_find = get_hashes_to_find( dataset, col_names )

        for file in files:
            find_vertices( '/'.join( [path,file] ), dataset, hashes_to_find )

            # checked, over?
            if len( hashes_to_find ) == 0:
                break   # done

        log.info( 'Done' )

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

    sql = 'SELECT id,name,max_degree_vertex,max_pagerank_vertex,max_in_degree_vertex,max_out_degree_vertex,pseudo_diameter_src_vertex,pseudo_diameter_trg_vertex,path_edgelist,path_graph_gt FROM stats_graph WHERE '+ names_query +' AND (max_degree_vertex IS NOT NULL OR max_pagerank_vertex IS NOT NULL) ORDER BY id'

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
        global_hashes = pickle.load( pkl_file )
    else:
        global_hashes = {}

    # setup threading
    sem = threading.Semaphore( args['processes'] )
    threads = []

    for dataset in datasets:

        t = threading.Thread( target = job_find_vertices, name = dataset['name'], args = ( dataset, sem ) )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    
    pkl_file = open( 'found_hashes.pkl', 'wb')
    pickle.dump( global_hashes, pkl_file )
