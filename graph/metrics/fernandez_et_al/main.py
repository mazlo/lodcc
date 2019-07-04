import re
import os
import argparse
import json
import logging as log
import threading
import sys

try:
    from graph_tool.all import *
except:
    print( 'graph_tool module could not be imported' )
import numpy as n
import powerlaw
n.warnings.filterwarnings('ignore')

import graph.metrics.fernandez_et_al.all as mf

lock = threading.Lock()

ROOT_DIR = os.path.abspath( os.curdir )

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

        if args['dict_hashed']:
            D=load_graph_from_csv( edgelist, directed=True, string_vals=False, hashed=False, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
        else:
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

    features = n.array( mf.all ).flatten()

    # one-time computation of edge-labels
    log.info( 'Preparing edge-label structure' )
    edge_labels = D.ep.c0.get_2d_array([0])[0]

    for ftr in features:
        ftr( D, stats, edge_labels )

        if not args['print_stats']:
            save_stats( dataset, stats )

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
        return

    # =========
    # CAUTION
    # please keep in mind that YOU CANNOT work with the vertice's and edge's index, 'cause it's a unique integer.
    # you have to work with the vertice's and edge's label in all operations
    # =========

    prop_s = D.new_vertex_property( 'bool', val=False )
    prop_o = D.new_vertex_property( 'bool', val=False )

    D.vertex_properties['subject'] = prop_s
    D.vertex_properties['object'] = prop_o

    for v in D.vertices():
        if v.out_degree() > 0:
            prop_s[v] = True
        if v.in_degree() > 0:
            prop_o[v] = True

    S = GraphView( D, vfilt=prop_s )
    O = GraphView( D, vfilt=prop_o )

    print( "Number of subjects: %s" % S.num_vertices() )
    print( "Number of objects: %s" % O.num_vertices() )

    return D

import datetime

# real job
def job_start_build_graph( dataset, sem, threads_openmp=7 ):
    """job_start_build_graph"""

    # let's go
    with sem:
        log.info( 'Let''s go' )
        log.debug( dataset )

        # init stats
        stats = dict( (attr, dataset[attr]) for attr in ['path_edgelist','path_graph_gt'] )

        # - build_graph_prepare
        D = build_graph_prepare( dataset, stats )

        start = datetime.datetime.now()
        # - build_graph_analyse
        build_graph_analyse( dataset, D, stats, threads_openmp )
        print( datetime.datetime.now() - start )

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

    actions.add_argument( '--build-graph', '-b', action = "store_true", help = '' )

    parser.add_argument( '--use-datasets', '-d', nargs='*', action = 'append', required = True, help = '' )
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
            'name': ds[0], 
            'path_edgelist': '%s/dumps/%s/data.edgelist.csv' % (ROOT_DIR, ds[0]), 
            'path_graph_gt': '%s/dumps/%s/data.graph.gt.gz' % (ROOT_DIR, ds[0]) }, datasets ) )
        
        names = ', '.join( map( lambda d: d['name'], datasets ) )
        log.debug( 'Configured datasets: %s', names )

        # init feature list
        if len( args['features'] ) == 0:
            # eigenvector_centrality, global_clustering and local_clustering left out due to runtime
            args['features'] = ['degree', 'plots', 'diameter', 'fill', 'h_index', 'pagerank', 'parallel_edges', 'powerlaw', 'reciprocity']

        build_graph( datasets, args['threads'], args['threads_openmp'] )
