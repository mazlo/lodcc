import argparse
import logging as log
import numpy as np
import os
import re

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

        for k in np.arange( 0.25, 1, 0.25 ):  # [ 0.25, 0.5, 0.75 ]
            if log:
                log.info( 'Taking %s sample edgelist..', k )

            sample  = np.random.choice( n, int( n.size*k ), replace=False )
            sample.sort()

            with open( 'data.edgelist.%s.csv' % k, 'w' ) as dataset_sampled:
                with open( dataset ) as f:
                    for i, line in enumerate(f):
                        for k, line_sample in enumerate(sample):
                            if line_sample > i:
                                break
                            if line_sample < i:
                                continue

                            dataset_sampled.write( line )
                            sample = sample[k:]
                            break

#
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - sample edgelist' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )

    log.basicConfig(
            level = log.INFO, 
            format = '[%(asctime)s] - %(levelname)-8s : %(message)s', )

    args = vars( parser.parse_args() )
    paths = args['paths']

    sample_edgelist( paths, log )

    log.info( 'done' )
