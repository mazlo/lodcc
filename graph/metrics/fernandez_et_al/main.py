import re
import os
import argparse
import json
import logging as log
import pandas as pd
import threading
import sys

try:
    from graph_tool.all import *
except:
    print( 'graph_tool module could not be imported' )
import numpy as np
import powerlaw
np.warnings.filterwarnings('ignore')

import db.helpers as db
import graph.metrics.fernandez_et_al.all as metrics

lock = threading.Lock()

ROOT_DIR = os.path.abspath( os.curdir )
DEFAULT_DATAFRAME_INDEX = [ 'time_overall' ]

def load_graph_from_edgelist( dataset ):
    """"""

    edgelist, graph_gt = dataset['path_edgelist'], dataset['path_graph_gt']

    D=None

    # prefer graph_gt file
    if graph_gt and os.path.isfile( graph_gt ):
        log.info( 'Constructing DiGraph from gt.xz' )
        D=load_graph( graph_gt )
    
    elif edgelist and os.path.isfile( edgelist ):
        log.info( 'Constructing DiGraph from edgelist' )
        D=load_graph_from_csv( edgelist, directed=True, string_vals=True, hashed=True, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
    
    else:
        log.error( 'edgelist or graph_gt file to read graph from does not exist' )
        return None

    return D

def graph_analyze_on_partitions( dataset, D, feature, stats ):
    """"""

    NO_PARTITIONS = args['partitions']
    log.info( 'Computing feature %s on %s partitions of the DiGraph' % ( feature.__name__, NO_PARTITIONS ) )

    if feature in metrics.SETS['SUBJECT_OUT_DEGREES'] \
        or feature in metrics.SETS['PREDICATE_LISTS'] \
        or feature in metrics.SETS['TYPED_SUBJECTS_OBJECTS']:
        
        # filter the graph for subjects, vertices with out-degree > 0
        S_G = GraphView( D, vfilt=lambda v:v.out_degree() > 0 )

        # we split up all subjects into X partitions. For example, 10 fragments of ~7600 vertices 
        # will result in this: [ [0,..,759], [760,.., 1519], .., [6840,7599] ]
        partitions = np.array_split( S_G.get_vertices(), NO_PARTITIONS )

        data = None
        for s_idx in np.arange( NO_PARTITIONS ):
            # now, we filter out those edges with source vertices from the current partition
            S_G_s = GraphView( D, efilt=np.isin( D.get_edges()[:,0], partitions[s_idx] ) )
            edge_labels = np.array( [ S_G_s.ep.c0[p] for p in S_G_s.edges() ] )

            # this should add up all the values we need later when computing the metric
            data = getattr( metrics, 'collect_'+ feature.__name__ )( S_G_s, edge_labels, data, {}, True )

        # compute metric from individual partitions
        getattr( metrics, 'reduce_'+ feature.__name__ )( data, S_G, stats )

    elif feature in metrics.SETS['OBJECT_IN_DEGREES']:
        # filter the graph for objects, vertices with in-degree > 0
        O_G = GraphView( D, vfilt=lambda v:v.in_degree() > 0 )

        # we split up all subjects into X partitions. For example, 10 fragments of ~7600 vertices 
        # will result in this: [ [0,..,759], [760,.., 1519], .., [6840,7599] ]
        partitions = np.array_split( O_G.get_vertices(), NO_PARTITIONS )

        data = None
        for o_idx in np.arange( NO_PARTITIONS ):
            # now, we filter out those edges with source vertices from the current partition
            O_G_s = GraphView( D, efilt=np.isin( D.get_edges()[:,1], partitions[o_idx] ) )
            edge_labels = np.array( [ O_G_s.ep.c0[p] for p in O_G_s.edges() ] )

            # this should add up all the values we need later when computing the metric
            data = metrics.object_in_degrees.collect_metric( feature, O_G_s, edge_labels, data, {}, True )

        # compute metric from individual partitions
        metrics.object_in_degrees.reduce_metric( data, stats, 'max_'+ feature.__name__, 'mean_'+ feature.__name__ )

    elif feature in metrics.SETS['PREDICATE_DEGREES']:
        # we first compute a unique set of predicates
        edge_labels = np.array( [D.ep.c0[p] for p in D.edges() ] )
        # and split up all predicates into X partitions. 
        partitions = np.array_split( np.unique( edge_labels ), NO_PARTITIONS )

        data = None
        for p_idx in np.arange( NO_PARTITIONS ):

            # now, we filter all edges with labels from the corresponding partition 
            P_G_s = GraphView( D, efilt=np.isin( edge_labels, partitions[p_idx] ) )
            # and use the edge labels from the current GraphView for the computation of the feature
            edge_labels_subgraph = np.array( [ P_G_s.ep.c0[p] for p in P_G_s.edges() ] )

            # this should add up all the values we need later when computing the metric
            data = metrics.predicate_degrees.collect_metric( feature, P_G_s, edge_labels_subgraph, data, {}, True )

        # compute metric from individual partitions
        metrics.predicate_degrees.reduce_metric( data, stats, 'max_'+ feature.__name__, 'mean_'+ feature.__name__ )

def graph_analyze( dataset, D, stats ):
    """
        CAUTION
        please keep in mind that YOU CANNOT work with the vertice's and edge's index, 'cause it's a unique integer.
        you have to work with the vertice's and edge's label in all operations
    """

    # final set of features is the intersection of both sets
    features = [ ftr_func for ftr_func in np.array( metrics.all ).flatten() if ftr_func.__name__ in args['features'] ]

    if len( features ) == 0:
        log.warn( 'Set of features to be computed is empty :/' )
        return

    NO_PARTITIONS = args['partitions']

    if NO_PARTITIONS <= 1:
        # one-time computation of edge-labels
        log.info( 'Preparing edge-label structure' )
        # we unfortunately need to iterate over all edges once, since the order of appearance of
        # edge labels together with subjects and objects is important
        edge_labels = np.array( [ D.ep.c0[p] for p in D.edges() ] )

    log.info( 'Computing features' )
    for ftr in features:
        
        if NO_PARTITIONS <= 1:
            # compute the feature on the whole graph
            ftr( D, edge_labels, stats, args['print_stats'] )

            if args['from_db']:
                db.save_stats( dataset, stats )

        else:
            # requested to partition the graph
            graph_analyze_on_partitions( dataset, D, ftr, stats )

def build_graph_analyse( dataset, D, stats, threads_openmp=7 ):
    """"""

    # before starting off: limit the number of threads a graph_tool job may acquire
    # TODO graph_tool.openmp_set_num_threads( threads_openmp )
    
    graph_analyze( dataset, D, stats )

    if args['print_stats']:
        print( ', '.join( [ key for key in stats.keys() ] ) )
        print( ', '.join( [ str(stats[key]) for key in stats.keys() ] ) )

def build_graph_prepare( dataset, stats ):
    """build_graph_prepare"""

    D=load_graph_from_edgelist( dataset )

    if not D:
        log.error( 'Could not instantiate graph, None' )
        return None

    return D

import datetime

# real job
def job_start_build_graph( dataset, dataframe, sem, threads_openmp=7 ):
    """job_start_build_graph"""

    # let's go
    with sem:
        log.info( 'Let''s go' )
        log.debug( dataset )

        # - build_graph_prepare
        stats = dict()
        D = build_graph_prepare( dataset, stats )

        if not D:
            log.error( 'Exiting due to graph None' )
            return

        # start timer
        start = datetime.datetime.now()

        # - build_graph_analyse
        build_graph_analyse( dataset, D, stats, threads_openmp )

        # save results
        stats['time_overall'] = datetime.datetime.now() - start
        dataframe[dataset['name']] = pd.Series( stats )

        # - job_cleanup

        log.info( 'Done' ) 

def build_graph( datasets, no_of_threads=1, threads_openmp=7 ):
    """"""

    if len( datasets ) == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    # init dataframe with index being all metrics + some defaults.
    # the transposed DataFrame is written to csv-file a results.
    dataframe = pd.DataFrame( index=metrics.LABELS + DEFAULT_DATAFRAME_INDEX )

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )
    threads = []

    for dataset in datasets:
        
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start_build_graph, name = dataset['name'], args = ( dataset, dataframe, sem, threads_openmp ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

    dataframe = dataframe.T.reset_index().rename( columns={ 'index': 'name' } )
    dataframe.to_csv( '%s/%s_results.csv' % (ROOT_DIR, datetime.datetime.now()), index_label='id' )

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )
    actions = parser.add_mutually_exclusive_group( required = True )

    actions.add_argument( '--build-graph', '-b', action = "store_true", help = '' )

    group = parser.add_mutually_exclusive_group( required = True )
    group.add_argument( '--from-db', '-fdb', action = "store_true", help = '' )
    group.add_argument( '--from-file', '-ffl', action = "store_true", help = '' )

    parser.add_argument( '--use-datasets', '-d', nargs='*', required = True, help = '' )
    parser.add_argument( '--print-stats', '-dp', action= "store_true", help = '' )
    
    parser.add_argument( '--log-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-info', '-li', action = "store_true", help = '' )
    parser.add_argument( '--threads', '-t', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

    # RE graph or feature computation
    parser.add_argument( '--threads-openmp', '-th', required = False, type = int, default = 7, help = 'Specify how many threads will be used for the graph analysis' )
    parser.add_argument( '--features', '-f', nargs='*', required = False, default = list(), help = '' )
    parser.add_argument( '--skip-features', '-fs', nargs='*', required = False, default = list(), help = '' )
    parser.add_argument( '--partitions', '-p', required = False, type = int, default = 1, help = 'If > 1, features will be computed on this number of partitions separately.' )    
    
    # args is available globaly
    args = vars( parser.parse_args() ).copy()

    # configure logging
    if args['log_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    # configure datasets
    datasets = args['use_datasets']        # argparse returns [[..], [..]]
    log.debug( 'Configured datasets: '+ ', '.join( datasets ) )

    # either from db
    if args['from_db']:
        log.debug( 'Requested to read data from db' )

        try:
            # connects, checks connection, and loads datasets
            db.init( args )
            db.connect()
        except:
            log.error( 'Database not ready for query execution. Check db.properties.\n Raised error: %s', sys.exc_info() )
            sys.exit(0)

        # read datasets
        names_query = '( ' + ' OR '.join( 'name = %s' for ds in datasets ) + ' )'
            
        if 'names_query' in locals():
            sql = ('SELECT id,name,path_edgelist,path_graph_gt FROM %s WHERE ' % args['db_tbname']) + names_query +' AND (path_edgelist IS NOT NULL OR path_graph_gt IS NOT NULL) ORDER BY id'
        else:
            sql = 'SELECT id,name,path_edgelist,path_graph_gt FROM %s WHERE (path_edgelist IS NOT NULL OR path_graph_gt IS NOT NULL) ORDER BY id' % args['db_tbname']
            
        datasets = db.run( sql, tuple( datasets ) )

    # or passed by cli arg
    elif args['from_file']:
        log.debug( 'Requested to read data from file' )

        # transform the cli arg list into object structure.
        # this format is compatible with the format that is returned by the database
        datasets = list( map( lambda ds: {
            'name': ds, 
            'path_edgelist': '%s/dumps/%s/data.edgelist.csv' % (ROOT_DIR, ds), 
            'path_graph_gt': '%s/dumps/%s/data.graph.gt.gz' % (ROOT_DIR, ds) }, datasets ) )

   # option 3
    if args['build_graph']:

        # init feature list
        if len( args['features'] ) == 0:
            #
            args['features'] = [ ftr.__name__ for ftr in np.array( metrics.all ).flatten() ]

        build_graph( datasets, args['threads'], args['threads_openmp'] )
