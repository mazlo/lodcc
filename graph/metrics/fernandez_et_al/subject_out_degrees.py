from graph_tool import GraphView
import numpy as np
import pandas as pd

# SUBJECT OUT-DEGREES

def out_degree( D, stats, edge_labels=None, print_stats=False ):
    """"""
    V = GraphView( D, efilt=D.get_edges()[:,0] )

    # the number of triples in G in which s occurs as subject
    l = V.get_out_degrees( V.get_vertices() ) + 0.0
    l[l == 0] = np.nan

    if print_stats:
        print( "(Eq.1) out-degree deg^{-}(s). max: %s, mean: %f" % ( np.nanmax(l), np.nanmean(l) ) )


def partial_out_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    # the number of triples of G, in which s occurs as subject and p as predicate
    ## e.g. l = [ ['/John','foaf:mbox'], ['/John','foaf:mbox'], ['/John','rdf:type'], ['/John','ex:birthPlace'], ['/Rome', 'foaf:name'], ['/Giacomo', 'ex:areaOfWork'], ['/Piero', 'ex:areaOfWork'] ]
    l = list( zip( 
            D.get_edges()[:,0], 
            edge_labels ) )

    _, l = np.unique( l, return_counts=True, axis=0 )

    if print_stats:
        print( "(Eq.2) partial out-degree deg^{--}(s,p). max: %s, mean: %f" % ( np.max( l ), np.mean( l ) ) )

def labelled_out_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    # the number of different predicates (labels) of G with which s is related as a subject
    l = zip( D.get_edges()[:,0], edge_labels )
    
    df = pd.DataFrame( 
        data=list(l), 
        index=np.arange(0, D.get_edges().shape[0]), 
        columns=np.arange(0, D.get_edges().shape[1]-1) )

    l = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.3) labelled out-degree deg^{-}_L(s). max: %s, mean: %f" % ( l.max(), l.mean() ) )
def direct_out_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    # the number of different objects of G with which s is related as a subject
    df = pd.DataFrame( 
        data=D.get_edges(), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange(0, D.get_edges().shape[1]) )

    l = df.groupby(0).nunique()[1]

all = [ out_degree, partial_out_degree, labelled_out_degree, direct_out_degree ]    if print_stats:
        print( "(Eq.4) direct out-degree deg^{-}_D(s). max: %s, mean: %f" % ( l.max(), l.mean() ) )
