import numpy as np
import pandas as pd

# SUBJECT OUT-DEGREES

def partial_out_degree( D, stats ):
    """"""

    # the number of triples of G, in which s occurs as subject and p as predicate
    ## e.g. l = [ ['/John','foaf:mbox'], ['/John','foaf:mbox'], ['/John','rdf:type'], ['/John','ex:birthPlace'], ['/Rome', 'foaf:name'], ['/Giacomo', 'ex:areaOfWork'], ['/Piero', 'ex:areaOfWork'] ]
    l = list( zip( 
            D.get_edges()[:,0], 
            D.ep.c0.get_2d_array( pos=[0])[0] ) )

    _, l = np.unique( l, return_counts=True, axis=0 )
    print( "(4) partial out-degree. max: %s, mean: %f" % ( np.max( l ), np.mean( l ) ) )

def labelled_out_degree( D, stats ):
    """"""

    # the number of different predicates (labels) of G with which s is related as a subject
    l = zip( 
        D.get_edges()[:,0], 
        D.ep.c0.get_2d_array([0])[0] )
    df = pd.DataFrame( data=list(l), index=np.arange(0, D.get_edges().shape[0]), columns=np.arange(0, D.get_edges().shape[1]-1) )

    l = df.groupby(0).nunique()[1]
    print( "(5) labelled out-degree. max: %s, mean: %s" % ( l.max(), l.mean() ) )

def direct_out_degree( D, stats ):
    """"""

    # the number of different objects of G with which s is related as a subject
    df = pd.DataFrame( 
        data=D.get_edges(), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange(0, D.get_edges().shape[1]) )

    l = df.groupby(0).nunique()[1]
    print( "(6) direct out-degree. max: %s, mean: %s" % ( l.max(), l.mean() ) )

all = [ partial_out_degree, labelled_out_degree, direct_out_degree ]