import argparse
import pandas as pd
import logging as log
import numpy as np
import os
import re
import threading

def sample_edgelist_job( dataset, df, n, k, sem ):
    """"""

    with sem:
        v_filt  = np.array( [False]*n )
       
        if log:
            log.info( 'Sampling edges ...' )

        v_rand  = np.random.choice( np.arange( n ), size=int( n*k ), replace=False )
        
        for e in v_rand:
            v_filt[e] = True

        sample_dir = os.path.dirname( dataset ) + '-sampled-%s' % k

        # e.g. dumps/core-sampled-0.25 gets creted if not present
        if not os.path.isdir( sample_dir ):
            if log:
                log.info( 'Creating directory ..' )

            os.mkdir( sample_dir )
       
        df.iloc[v_filt].to_csv( '%s/data.edgelist.csv' % sample_dir, sep=' ', header=False, index=False )

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
            
        df  = pd.read_csv( dataset, delim_whitespace=True, header=None )
        n   = df.shape[0]

        # prepare
        sem = threading.Semaphore( 10 )
        threads = []

        for k in np.linspace(0.05, 0.5, num=10):  # e.g. [ 0.25, 0.5, 0.75 ]

            t = threading.Thread( target = sample_edgelist_job, name = '%s[%s]' % ( os.path.dirname(dataset), k ), args = ( dataset, df, n, k, sem ) )
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
