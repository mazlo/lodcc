from graph.building import builder

def fs_digraph_start_job( dataset, D, stats ):
    """"""

    features = [ 
        # fs = feature set
        fs_digraph_using_basic_properties,
        fs_digraph_using_degree, fs_digraph_using_indegree,
        f_centralization,
        f_reciprocity,
        f_pseudo_diameter,
        f_avg_clustering,
        f_pagerank, 
        f_eigenvector_centrality,
    ]

    for ftr in features:
        ftr( D, stats )

        if not args['print_stats'] and not args['from_file']:
            save_stats( dataset, stats )

def fs_ugraph_start_job( dataset, U, stats ):
    """"""

    features = [ 
        # fs = feature set
        f_global_clustering, #f_avg_clustering, 
        # f_avg_shortest_path, 
    ]

    for ftr in features:
        ftr( U, stats )

        if not args['print_stats'] and not args['from_file']:
            save_stats( dataset, stats )

def graph_analyze( dataset, stats ):
    """"""
   
    D = builder.load_graph_from_edgelist( dataset )

    if not D:
        log.error( 'Could not instantiate graph, None' )
        return

    log.info( 'Computing feature set DiGraph' )
    fs_digraph_start_job( dataset, D, stats )
    
    D.set_directed(False)
    log.info( 'Computing feature set UGraph' )
    fs_ugraph_start_job( dataset, D, stats )
    
    # slow
    #stats['k_core(U)']=nx.k_core(U)
    #stats['radius(U)']=nx.radius(U)
    
    return stats

def build_graph_analyse( dataset, threads_openmp=7 ):
    """"""

    # before starting off: limit the number of threads a graph_tool job may acquire
    graph_tool.openmp_set_num_threads( threads_openmp )

    # init stats
    
    stats = dict( (attr, dataset[attr]) for attr in ['path_edgelist','path_graph_gt'] )
    graph_analyze( dataset, stats )

    if args['print_stats']:
        if args['from_file']:
            print( ', '.join( [ key for key in stats.keys() ] ) )
            print( ', '.join( [ str(stats[key]) for key in stats.keys() ] ) )
        else:
            print( stats )

# real job
def job_start_build_graph( dataset, sem, threads_openmp=7 ):
    """job_start_build_graph"""

    # let's go
    with sem:
        log.info( 'Let''s go' )
        log.debug( dataset )

        # - build_graph_analyse
        build_graph_analyse( dataset, threads_openmp )

        # - job_cleanup

        log.info( 'Done' ) 

def build_graph( datasets, no_of_threads=1, threads_openmp=7 ):
    """"""

    if len( datasets ) == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )
    threads = []

    for dataset in datasets:
        
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start_build_graph, name = dataset['name'], args = ( dataset, sem, threads_openmp ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )
    actions = parser.add_mutually_exclusive_group( required = True )

    actions.add_argument( '--build-graph', '-pa', action = "store_true", help = '' )
    
    parser.add_argument( '--dry-run', '-d', action = "store_true", help = '' )

    parser.add_argument( '--use-datasets', '-du', nargs='*', help = '' )
    parser.add_argument( '--overwrite-dl', '-ddl', action = "store_true", help = 'If this argument is present, the program WILL NOT use data dumps which were already dowloaded, but download them again' )
    parser.add_argument( '--overwrite-nt', '-dnt', action = "store_true", help = 'If this argument is present, the program WILL NOT use ntriple files which were already transformed, but transform them again' )
    parser.add_argument( '--rm-original', '-dro', action = "store_true", help = 'If this argument is present, the program WILL REMOVE the original downloaded data dump file' )
    parser.add_argument( '--keep-edgelists', '-dke', action = "store_true", help = 'If this argument is present, the program WILL KEEP single edgelists which were generated. A data.edgelist.csv file will be generated nevertheless.' )
    
    group = parser.add_mutually_exclusive_group( required = True )
    group.add_argument( '--from-db', '-fdb', action = "store_true", help = '' )
    group.add_argument( '--from-file', '-ffl', action = "append", help = '', nargs = '*')

    parser.add_argument( '--log-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-info', '-li', action = "store_true", help = '' )
    parser.add_argument( '--log-file', '-lf', action = "store_true", help = '' )
    parser.add_argument( '--print-stats', '-lp', action= "store_true", help = '' )
    parser.add_argument( '--threads', '-pt', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

    # TODO add --sample-edges
    parser.add_argument( '--sample-vertices', '-gsv', action = "store_true", help = '' )
    parser.add_argument( '--sample-size', '-gss', required = False, type = float, default = 0.2, help = '' )

    # RE graph or feature computation
    parser.add_argument( '--dump-graph', '-gs', action = "store_true", help = '' )
    parser.add_argument( '--reconstruct-graph', '-gr', action = "store_true", help = '' )
    parser.add_argument( '--dict-hashed', '-gh', action = "store_true", help = '' )
    parser.add_argument( '--threads-openmp', '-gth', required = False, type = int, default = 7, help = 'Specify how many threads will be used for the graph analysis' )
    parser.add_argument( '--do-heavy-analysis', '-gfsh', action = "store_true", help = '' )
    parser.add_argument( '--features', '-gfs', nargs='*', required = False, default = list(), help = '' )
    parser.add_argument( '--skip-features', '-gsfs', nargs='*', required = False, default = list(), help = '' )
    
    # args is available globaly
    args = vars( parser.parse_args() ).copy()

    # configure logging
    if args['log_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    if args['log_file']:
        log.basicConfig( filename = 'lodcc.log', filemode='w', level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )
    else:
        log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    # read all properties in file into args-dict
    if args['from_db']:
        log.debug( 'Requested to read data from db' )

        if not os.path.isfile( 'db.properties' ):
            log.error( '--from-db given but no db.properties file found. please specify.' )
            sys.exit(0)
        else:
            with open( 'db.properties', 'rt' ) as f:
                args.update( dict( ( key.replace( '.', '_' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) ) )
    
    elif args['from_file']:
        log.debug( 'Requested to read data from file' )

    if args['from_db']:
        # connect to an existing database
        conn = psycopg2.connect( host=args['db_host'], dbname=args['db_dbname'], user=args['db_user'], password=args['db_password'] )
        conn.set_session( autocommit=True )

        try:
            cur = conn.cursor()
            cur.execute( "SELECT 1;" )
            result = cur.fetchall()
            cur.close()

            log.debug( 'Database ready to query execution' )
        except:
            log.error( 'Database not ready for query execution. Check db.properties. db error:\n %s', sys.exc_info()[0] )
            raise 

    # option 3
    if args['build_graph'] or args['dump_graph']:

        if args['from_db']:
            # respect --use-datasets argument
            if args['use_datasets']:
                names_query = '( ' + ' OR '.join( 'name = %s' for ds in args['use_datasets'] ) + ' )'
                names = tuple( args['use_datasets'] )
            else:
                names = 'all'

            if args['dry_run']:
                log.info( 'Running in dry-run mode' )

                # if not given explicitely above, shrink available datasets to one special
                if not args['use_datasets']:
                    names_query = 'name = %s'
                    names = tuple( ['museums-in-italy'] )

            log.debug( 'Configured datasets: '+ ', '.join( names ) )

            if 'names_query' in locals():
                sql = ('SELECT id,name,path_edgelist,path_graph_gt FROM %s WHERE ' % args['db_tbname']) + names_query +' AND (path_edgelist IS NOT NULL OR path_graph_gt IS NOT NULL) ORDER BY id'
            else:
                sql = 'SELECT id,name,path_edgelist,path_graph_gt FROM %s WHERE (path_edgelist IS NOT NULL OR path_graph_gt IS NOT NULL) ORDER BY id' % args['db_tbname']
            
            cur = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cur.execute( sql, names )

            datasets = cur.fetchall()
            cur.close()

        else:
            datasets = args['from_file']        # argparse returns [[..], [..]]
            datasets = list( map( lambda ds: {        # to be compatible with existing build_graph function we transform the array to a dict
                'name': ds[0], 
                'path_edgelist': 'dumps/%s/data.edgelist.csv' % ds[0], 
                'path_graph_gt': 'dumps/%s/data.graph.gt.gz' % ds[0] }, datasets ) )
            
            names = ', '.join( map( lambda d: d['name'], datasets ) )
            log.debug( 'Configured datasets: %s', names )

        if args['build_graph']:

            # init feature list
            if len( args['features'] ) == 0:
                # eigenvector_centrality, global_clustering and local_clustering left out due to runtime
                args['features'] = ['degree', 'plots', 'diameter', 'fill', 'h_index', 'pagerank', 'parallel_edges', 'powerlaw', 'reciprocity']

            build_graph( datasets, args['threads'], args['threads_openmp'] )

        elif args['dump_graph']:
            # this is only respected when --dump-graph is specified without --build-graph (that's why the elif)
            # --dump-graph is respected in the build_graph function, when specified together with --build-graph.
            
            # TODO ZL respect --file-file
            datasets = cur.fetchall()
            cur.close()

            for ds in datasets:
                g = builder.load_graph_from_edgelist( ds )

                if not g:
                    log.error( 'Could not instantiate graph for dataset %s', ds['name'] )
                    continue

                graph_gt = builder.dump_graph( ds['path_edgelist'] )
                stats = { 'path_graph_gt' : graph_gt }

                # thats it here
                save_stats( ds, stats )
                continue

    # close communication with the database
    if args['from_db']:
        conn.close()