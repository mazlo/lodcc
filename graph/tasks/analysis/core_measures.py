import argparse
import logging
import threading

from db.SqliteHelper import SqliteHelper

from graph.building import builder
from graph.metrics.core.basic_measures import fs_digraph_using_basic_properties
from graph.metrics.core.degree_based import fs_digraph_using_degree, fs_digraph_using_indegree
from graph.metrics.core.edge_based import f_reciprocity, f_pseudo_diameter
from graph.metrics.core.centrality import f_centralization, f_eigenvector_centrality, f_pagerank
from graph.metrics.core.clustering import f_global_clustering, f_local_clustering

log = logging.getLogger( __name__ )

import graph_tool

def fs_digraph_start_job( dataset, D, stats, options ):
    """"""

    features = [ 
        # fs = feature set
        fs_digraph_using_basic_properties,
        fs_digraph_using_degree, fs_digraph_using_indegree,
        f_centralization,
        f_reciprocity,
        f_pseudo_diameter,
        f_local_clustering,
        f_pagerank, 
        f_eigenvector_centrality,
    ]

    for ftr in features:
        ftr( D, stats, options )

        if not args['print_stats'] and not args['from_file']:
            save_stats( dataset, stats )

def fs_ugraph_start_job( dataset, U, stats, options ):
    """"""

    features = [ 
        # fs = feature set
        f_global_clustering, #f_avg_clustering, 
        # f_avg_shortest_path, 
    ]

    for ftr in features:
        ftr( U, stats, options )

        if not args['print_stats'] and not args['from_file']:
            save_stats( dataset, stats )

def graph_analyze( dataset, stats, options ):
    """"""
   
    D = builder.load_graph_from_edgelist( dataset )

    if not D:
        log.error( 'Could not instantiate graph, None' )
        return

    log.info( 'Computing feature set DiGraph' )
    fs_digraph_start_job( dataset, D, stats, options )
    
    D.set_directed(False)
    log.info( 'Computing feature set UGraph' )
    fs_ugraph_start_job( dataset, D, stats, options )
    
    # slow
    #stats['k_core(U)']=nx.k_core(U)
    #stats['radius(U)']=nx.radius(U)
    
    return stats

def build_graph_analyse( dataset, options ):
    """"""

    # before starting off: limit the number of threads a graph_tool job may acquire
    if args['openmp_enabled']:
        graph_tool.openmp_set_num_threads( options['threads_openmp'] )

    # init stats
    stats = dict( (attr, dataset[attr]) for attr in ['path_edgelist','path_graph_gt'] )

    graph_analyze( dataset, stats, options )

    if args['print_stats']:
        if args['from_file']:
            print( ', '.join( [ key for key in stats.keys() ] ) )
            print( ', '.join( [ str(stats[key]) for key in stats.keys() ] ) )
        else:
            print( stats )

# real job
def job_start_build_graph( dataset, sem, options ):
    """job_start_build_graph"""

    # let's go
    with sem:
        log.info( 'Let''s go' )
        log.debug( dataset )

        # - build_graph_analyse
        build_graph_analyse( dataset, options )

        # - job_cleanup

        log.info( 'Done' ) 

def build_graph( datasets, options ):
    """"""

    if len( datasets ) == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if options['threads'] <= 0 else ( 20 if options['threads'] > 20 else options['threads'] ) ) )
    threads = []

    for dataset in datasets:
        
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start_build_graph, name = dataset['name'], args = ( dataset, sem, options ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )

    group = parser.add_mutually_exclusive_group( required = True )
    group.add_argument( '--from-db', '-fdb', type = str, nargs='+', help = '' )
    group.add_argument( '--from-file', '-ffl', action = "append", help = '', nargs = '*')

    parser.add_argument( '--print-stats', '-lp', action= "store_true", help = '' )
    parser.add_argument( '--threads', '-pt', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

    # TODO add --sample-edges
    parser.add_argument( '--sample-vertices', '-gsv', action = "store_true", help = '' )
    parser.add_argument( '--sample-size', '-gss', required = False, type = float, default = 0.2, help = '' )

    # RE graph or feature computation
    parser.add_argument( '--openmp-enabled', '-gto', action = "store_true", help = '' )
    parser.add_argument( '--threads-openmp', '-gth', required = False, type = int, default = 7, help = 'Specify how many threads will be used for the graph analysis' )
    parser.add_argument( '--do-heavy-analysis', '-gfsh', action = "store_true", help = '' )
    parser.add_argument( '--features', '-gfs', nargs='*', required = False, default = list(), help = '' )
    parser.add_argument( '--skip-features', '-gsfs', nargs='*', required = False, default = list(), help = '' )
    
    # args is available globaly
    args = vars( parser.parse_args() ).copy()

    if args['from_db']:
        log.info( 'Requested to prepare graph from db' )
        db = SqliteHelper()

        # respect --use-datasets argument
        log.debug( 'Configured datasets: ' + ', '.join( args['from_db'] ) )
        datasets = db.get_datasets_and_paths( args['from_db'] )
    else:
        datasets = args['from_file']        # argparse returns [[..], [..]]
        datasets = list( map( lambda ds: {  # to be compatible with existing build_graph function we transform the array to a dict
            'name': ds[0], 
            'path_edgelist': 'dumps/%s/data.edgelist.csv' % ds[0], 
            'path_graph_gt': 'dumps/%s/data.graph.gt.gz' % ds[0] }, datasets ) )
        
        names = ', '.join( map( lambda d: d['name'], datasets ) )
        log.debug( 'Configured datasets: %s', names )

    # init feature list
    if len( args['features'] ) == 0:
        # eigenvector_centrality, global_clustering and local_clustering left out due to runtime
        args['features'] = ['degree', 'plots', 'diameter', 'fill', 'h_index', 'pagerank', 'parallel_edges', 'powerlaw', 'reciprocity']

    build_graph( datasets, dict( ( k,args[k] ) for k in ['features','threads','threads_openmp'] ) )