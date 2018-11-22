import argparse
import logging as log
import numpy as np
from graph_tool import *
import os
import threading

def sample_vertices_job( dataset, k, sem ):
    """creates a sampled sub graph from dataset with k vertices and corresponding edges"""

    with sem:
        if log:
            log.info( 'Reconstructing graph ...')

        # dataset e.g. 'dumps/education-data-gov-uk/data.graph.gt.gz'
        D = load_graph( dataset )
        
        vfilt   = D.new_vertex_property( 'bool' )
        v       = D.get_vertices()

        if log:
            log.info( 'Sampling vertices ...')

        v_rand  = np.random.choice( v, size=int( len(v)*k ), replace=False )
        
        for e in v_rand:
            vfilt[e] = True
        
        if log:
            log.info( 'Saving subgraph ...' )

        D_sub   = GraphView( D, vfilt=vfilt )
        
        # e.g. 'dumps/education-data-gov-uk/data.graph.0.25.gt.gz'
        graph_gt = '/'.join( [os.path.dirname( dataset ), 'data.graph.%s.gt.gz' % k] )
        D_sub.save( graph_gt )

def sample_vertices( paths, log=None ):
    """"""

    # ensure it is a list
    if not type(paths) is list:
        paths = [paths]

    for dataset in paths:
        if not os.path.isfile( dataset ):
            dataset = 'dumps/'+ dataset

            if not os.path.isdir( dataset ):
                if log:
                    log.error( '%s is not a directory', dataset )
                continue

            dataset = dataset + '/data.graph.gt.gz'

            if not os.path.isfile( dataset ):
                if log:
                    log.error( 'graph file does not exit (was looking in %s). this is a requirement', dataset )
                continue

        # prepare
        sem = threading.Semaphore( 10 )
        threads = []

        for k in np.linspace(0.05, 0.5, num=10):  # e.g. [ 0.25, 0.5, 0.75 ]

            t = threading.Thread( target = sample_vertices_job, name = '%s[%s]' % ( os.path.dirname(dataset), k ), args = ( dataset, k, sem ) )
            t.start()

            threads.append( t )

        # wait for all threads to finish
        for t in threads:
            t.join()

#
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - sample vertices' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )

    log.basicConfig(
            level = log.INFO, 
            format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    args = vars( parser.parse_args() )
    paths = args['paths']

    sample_vertices( paths, log )

    log.info( 'done' )
