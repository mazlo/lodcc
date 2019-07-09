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

def graph_analyze( dataset, D, stats ):
    """
        CAUTION
        please keep in mind that YOU CANNOT work with the vertice's and edge's index, 'cause it's a unique integer.
        you have to work with the vertice's and edge's label in all operations
    """

    features = np.array( metrics.all ).flatten()

    # one-time computation of edge-labels
    log.info( 'Preparing edge-label structure' )
    edge_labels = D.ep.c0.get_2d_array([0])[0]

    log.info( 'Computing features' )
    for ftr in features:
        ftr( D, stats, edge_labels )

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

    parser.add_argument( '--use-datasets', '-d', nargs='*', required = True, help = '' )
    parser.add_argument( '--print-stats', '-dp', action= "store_true", help = '' )
    
    parser.add_argument( '--log-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-info', '-li', action = "store_true", help = '' )
    parser.add_argument( '--threads', '-t', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

    # RE graph or feature computation
    parser.add_argument( '--threads-openmp', '-th', required = False, type = int, default = 7, help = 'Specify how many threads will be used for the graph analysis' )
    parser.add_argument( '--features', '-f', nargs='*', required = False, default = list(), help = '' )
    parser.add_argument( '--skip-features', '-fs', nargs='*', required = False, default = list(), help = '' )
    
    # args is available globaly
    args = vars( parser.parse_args() ).copy()

    # configure logging
    if args['log_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

   # option 3
    if args['build_graph']:

        datasets = args['use_datasets']        # argparse returns [[..], [..]]
        datasets = list( map( lambda ds: {        # to be compatible with existing build_graph function we transform the array to a dict
            'name': ds, 
            'path_edgelist': '%s/dumps/%s/data.edgelist.csv' % (ROOT_DIR, ds), 
            'path_graph_gt': '%s/dumps/%s/data.graph.gt.gz' % (ROOT_DIR, ds) }, datasets ) )
        
        names = ', '.join( map( lambda d: d['name'], datasets ) )
        log.debug( 'Configured datasets: %s', names )

        # init feature list
        if len( args['features'] ) == 0:
            # eigenvector_centrality, global_clustering and local_clustering left out due to runtime
            args['features'] = ['degree', 'plots', 'diameter', 'fill', 'h_index', 'pagerank', 'parallel_edges', 'powerlaw', 'reciprocity']

        build_graph( datasets, args['threads'], args['threads_openmp'] )
