from graph_tool import GraphView
import numpy as np
import pandas as pd

def in_degree( D, stats, edge_labels=None ):
    """"""
    V = GraphView( D, efilt=D.get_edges()[:,1] )

    # the number of triples in G in which o occurs as object
    l = V.get_in_degrees( V.get_vertices() ) + 0.0
    l[l == 0] = np.nan
    print( "(Eq.5) in-degree deg^{+}(o). max: %s, mean: %f" % ( np.nanmax(l), np.nanmean(l) ) )

def partial_in_degree( D, stats, edge_labels=np.empty(0) ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    # the number of triples of G, in which o occurs as object and p as predicate
    ## e.g. l = ['foaf:mbox_john@example.org', 'foaf:mbox_john@doe.org', 'rdf:type_/Researcher', 'ex:areaOfWork_/Rome', 'ex:areaOfWork_/Rome', 'ex:birthPlace_/Rome', 'foaf:name_"Roma"@it']
    l = list( zip( 
            D.get_edges()[:,1], 
            edge_labels ) )

    _, l = np.unique( l, return_counts=True, axis=0 )
    print( "(Eq.6) partial in-degree deg^{++}(o,p). max: %s, mean: %f" % ( np.max( l ), np.mean( l ) ) )

def labelled_in_degree( D, stats, edge_labels=np.empty(0) ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    # the number of different predicates (labels) of G with which o is related as a object
    l = zip( D.get_edges()[:,1], edge_labels )
    
    df = pd.DataFrame( 
        data=list(l), 
        index=np.arange(0, D.get_edges().shape[0]), 
        columns=np.arange(0, D.get_edges().shape[1]-1) )

    l = df.groupby(0).nunique()[1]
    print( "(Eq.7) labelled in-degree deg^{+}_L(s). max: %s, mean: %f" % ( l.max(), l.mean() ) )

def direct_in_degree( D, stats, edge_labels=np.empty(0) ):
    """"""

    # the number of different subjects of G with which o is related as a object
    df = pd.DataFrame( 
        data=D.get_edges(), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange(0, D.get_edges().shape[1]) )

    l = df.groupby(1).nunique()[0]
    print( "(Eq.8) direct in-degree deg^{+}_D(o). max: %s, mean: %f" % ( l.max(), l.mean() ) )

all = [ in_degree, partial_in_degree, labelled_in_degree, direct_in_degree ]
