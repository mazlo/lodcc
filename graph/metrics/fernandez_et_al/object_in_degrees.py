import numpy as np
import pandas as pd

def in_degree( D, edge_labels=None, stats=dict(), print_stats=False ):
    """"""
    # the number of triples in G in which o occurs as object
    l = D.get_in_degrees( D.get_vertices() ) + 0.0
    l[l == 0] = np.nan

    if print_stats:
        print( "(Eq.5) in-degree deg^{+}(o). max: %s, mean: %f" % ( np.nanmax(l), np.nanmean(l) ) )

    stats['max_in_degree'], stats['mean_in_degree'] = np.nanmax(l), np.nanmean(l)

    return l

def partial_in_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    if edge_labels is None or edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of triples of G, in which o occurs as object and p as predicate
    ## e.g. l = ['foaf:mbox_john@example.org', 'foaf:mbox_john@doe.org', 'rdf:type_/Researcher', 'ex:areaOfWork_/Rome', 'ex:areaOfWork_/Rome', 'ex:birthPlace_/Rome', 'foaf:name_"Roma"@it']
    l = list( zip( D.get_edges()[:,1], edge_labels ) )
    _, l = np.unique( l, return_counts=True, axis=0 )

    if print_stats:
        print( "(Eq.6) partial in-degree deg^{++}(o,p). max: %s, mean: %f" % ( np.max( l ), np.mean( l ) ) )

    stats['max_partial_in_degree'], stats['mean_partial_in_degree'] = np.max( l ), np.mean( l )

    return l

def labelled_in_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    if edge_labels is None or edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of different predicates (labels) of G with which o is related as a object
    df = pd.DataFrame( 
        data=list( zip( D.get_edges()[:,1], edge_labels ) ), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.7) labelled in-degree deg^{+}_L(s). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_labelled_in_degree'], stats['mean_labelled_in_degree'] = df.max(), df.mean()

    return df

def direct_in_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    # the number of different subjects of G with which o is related as a object
    df = pd.DataFrame( 
        data=D.get_edges(), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(1).nunique()[0]

    if print_stats:
        print( "(Eq.8) direct in-degree deg^{+}_D(o). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_direct_in_degree'], stats['mean_direct_in_degree'] = df.max(), df.mean()

    return df

def collect_metric( feature, O_G_s, edge_labels, vals, stats, print_stats ):
    """"""
    if vals is None:
        vals = np.empty(0)

    return np.append( vals, feature( O_G_s, edge_labels, stats, print_stats ) )

def reduce_metric( vals, stats, max_metric, mean_metric ):
    """"""
    stats[max_metric], stats[mean_metric] = np.nanmax(vals), np.nanmean(vals)

METRICS     = [ in_degree, partial_in_degree, labelled_in_degree, direct_in_degree ]
METRICS_SET = { 'OBJECT_IN_DEGREES': METRICS }
LABELS      = [ 'max_in_degree', 'mean_in_degree', 'max_partial_in_degree', 'mean_partial_in_degree', 'max_labelled_in_degree', 'mean_labelled_in_degree', 'max_direct_in_degree', 'mean_direct_in_degree' ]
