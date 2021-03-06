import numpy as np
import pandas as pd

def predicate_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    if edge_labels is None or edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # number of triples of graph G, in which p occurs as predicate
    _, l = np.unique( edge_labels, return_counts=True )

    if print_stats:
        print( "(Eq.9) predicate degree deg_P(p). max: %s, mean: %f" % ( np.max(l), np.mean(l) ) )

    stats['max_predicate_degree'], stats['mean_predicate_degree'] = np.max(l), np.mean(l)

    return l

def predicate_in_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    if edge_labels is None or edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of different subjects of G with which p is related as a predicate
    df = pd.DataFrame( 
        data=list( zip ( edge_labels, D.get_edges()[:,0] ) ), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.10) predicate in-degree deg^{+}_P(p). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_predicate_in_degree'], stats['mean_predicate_in_degree'] = df.max(), df.mean()

    return df

def predicate_out_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    if edge_labels is None or edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of different objects of G with which p is related as a predicate
    df = pd.DataFrame( 
        data=list( zip ( edge_labels, D.get_edges()[:,1] ) ), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.11) predicate out-degree deg^{-}_P(p). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_predicate_out_degree'], stats['mean_predicate_out_degree'] = df.max(), df.mean()

    return df

def collect_metric( feature, P_G, edge_labels, vals, stats, print_stats ):
    """"""
    if vals is None:
        vals = np.empty(0)

    return np.append( vals, feature( P_G, edge_labels, stats, print_stats ) )

def reduce_metric( vals, stats, max_metric_name, mean_metric_name ):
    """"""
    stats[max_metric_name], stats[mean_metric_name] = np.nanmax(vals), np.nanmean(vals)

METRICS     = [ predicate_degree, predicate_in_degree, predicate_out_degree ]
METRICS_SET = { 'PREDICATE_DEGREES': METRICS }
LABELS      = [ 'max_predicate_degree', 'mean_predicate_degree', 'max_predicate_in_degree', 'mean_predicate_in_degree', 'max_predicate_out_degree', 'mean_predicate_out_degree' ]
