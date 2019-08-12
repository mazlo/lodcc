import numpy as np
import pandas as pd

# SUBJECT OUT-DEGREES

def out_degree( D, edge_labels=None, stats=dict(), print_stats=False ):
    """"""

    # the number of triples in G in which s occurs as subject
    l = D.get_out_degrees( D.get_vertices() ) + 0.0
    l[l == 0] = np.nan

    if print_stats:
        print( "(Eq.1) out-degree deg^{-}(s). max: %s, mean: %f" % ( np.nanmax(l), np.nanmean(l) ) )

    stats['max_out_degree'], stats['mean_out_degree'] = np.nanmax(l), np.nanmean(l)

    return l

def partial_out_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of triples of G, in which s occurs as subject and p as predicate
    ## e.g. l = [ ['/John','foaf:mbox'], ['/John','foaf:mbox'], ['/John','rdf:type'], ['/John','ex:birthPlace'], ['/Rome', 'foaf:name'], ['/Giacomo', 'ex:areaOfWork'], ['/Piero', 'ex:areaOfWork'] ]
    l = list( zip( D.get_edges()[:,0], edge_labels ) )
    _, l = np.unique( l, return_counts=True, axis=0 )

    if print_stats:
        print( "(Eq.2) partial out-degree deg^{--}(s,p). max: %s, mean: %f" % ( np.max( l ), np.mean( l ) ) )

    stats['max_partial_out_degree'], stats['mean_partial_out_degree'] = np.max( l ), np.mean( l )

    return l

def labelled_out_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of different predicates (labels) of G with which s is related as a subject
    df = pd.DataFrame( 
        data=list( zip( D.get_edges()[:,0], edge_labels ) ), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.3) labelled out-degree deg^{-}_L(s). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_labelled_out_degree'], stats['mean_labelled_out_degree'] = df.max(), df.mean()

    return df

def direct_out_degree( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """"""

    # the number of different objects of G with which s is related as a subject
    df = pd.DataFrame( 
        data=D.get_edges(), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.4) direct out-degree deg^{-}_D(s). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_direct_out_degree'], stats['mean_direct_out_degree'] = df.max(), df.mean()

    return df

def collect_metric( feature, S_G_s, edge_labels, vals, stats, print_stats ):
    """"""
    if vals is None:
        vals = np.empty(0)

    return np.append( vals, feature( S_G_s, edge_labels, stats, print_stats ) )

def reduce_metric( vals, stats, max_metric, mean_metric ):
    """"""
    stats[max_metric], stats[mean_metric] = np.nanmax(vals), np.nanmean(vals)

###

def collect_out_degree( S_G_s, edge_labels, vals=np.empty(0), stats=dict(), print_stats=False ):
    """"""
    return collect_metric( out_degree, S_G_s, edge_labels, vals, stats, print_stats )

def collect_partial_out_degree( S_G_s, edge_labels, vals=np.empty(0), stats=dict(), print_stats=False ):
    """"""
    return collect_metric( partial_out_degree, S_G_s, edge_labels, vals, stats, print_stats )

def collect_labelled_out_degree( S_G_s, edge_labels, vals=pd.DataFrame(), stats=dict(), print_stats=False ):
    """"""
    return collect_metric( labelled_out_degree, S_G_s, edge_labels, vals, stats, print_stats )

def collect_direct_out_degree( S_G_s, edge_labels, vals=pd.DataFrame(), stats=dict(), print_stats=False ):
    """"""
    return collect_metric( direct_out_degree, S_G_s, edge_labels, vals, stats, print_stats )

def reduce_out_degree( vals, G, stats ):
    """"""
    reduce_metric( vals, stats, 'max_out_degree', 'mean_out_degree' )

def reduce_partial_out_degree( vals, G, stats ):
    """"""
    reduce_metric( vals, stats, 'max_partial_out_degree', 'mean_partial_out_degree' )

def reduce_labelled_out_degree( vals, G, stats ):
    """"""
    reduce_metric( vals, stats, 'max_labelled_out_degree', 'mean_labbelled_out_degree' )

def reduce_direct_out_degree( vals, G, stats ):
    """"""
    reduce_metric( vals, stats, 'max_direct_out_degree', 'mean_direct_out_degree' )

METRICS     = [ out_degree, partial_out_degree, labelled_out_degree, direct_out_degree ]
METRICS_SET = { 'SUBJECT_OUT_DEGREES' : METRICS  }
LABELS      = [ 'max_out_degree', 'mean_out_degree', 'max_partial_out_degree', 'mean_partial_out_degree', 'max_labelled_out_degree', 'mean_labelled_out_degree', 'max_direct_out_degree', 'mean_direct_out_degree' ]