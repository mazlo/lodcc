import argparse
import logging as log
import numpy as np
import os
import re
import threading

def sample_edgelist_job( dataset, n, k, sem ):
    """"""

    with sem:
        if log:
            log.info( 'Taking sample ..' )

        sample  = np.random.choice( n, int( n.size*k ), replace=False )
        sample.sort()
        
        sample_dir = os.path.dirname( dataset ) + '-sampled-%s' % k

        # e.g. dumps/core-sampled-0.25 gets creted if not present
        if not os.path.isdir( sample_dir ):
            if log:
                log.info( 'Creating directory ..' )
            os.mkdir( sample_dir )
        
        with open( '%s/data.edgelist.csv' % sample_dir, 'w' ) as dataset_sampled:
            with open( dataset ) as f:
                if log:
                    log.info( 'Sampling edgelist ..' )
                for i, line in enumerate(f):
                    for k, line_sample in enumerate(sample):
                        if line_sample > i:
                            break
                        if line_sample < i:
                            continue
                        
                        dataset_sampled.write( line )
                        sample = sample[k:]
                        break

        if log:
            log.info( 'Done' )
                
def sample_edgelist( paths, log=None ):
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

            dataset = dataset + '/data.edgelist.csv'

            if not os.path.isfile( dataset ):
                if log:
                    log.error( 'Edgelist file does not exit (was looking in %s). this is a requirement', dataset )
                continue

        if log:
            log.info( 'Reading lines ..' )
            
        num_lines = sum( 1 for line in open( dataset ) )
        n         = np.arange( num_lines ) + 1

        # prepare
        sem = threading.Semaphore( 10 )
        threads = []

        for k in np.linspace(0.05, 0.5, num=10):  # e.g. [ 0.25, 0.5, 0.75 ]

            t = threading.Thread( target = sample_edgelist_job, name = '%s[%s]' % ( os.path.dirname(dataset), k ), args = ( dataset, n, k, sem ) )
            t.start()

            threads.append( t )

        # wait for all threads to finish
        for t in threads:
            t.join()

#
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - sample edgelist' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )

    log.basicConfig(
            level = log.INFO, 
            format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    args = vars( parser.parse_args() )
    paths = args['paths']

    sample_edgelist( paths, log )

    log.info( 'done' )
