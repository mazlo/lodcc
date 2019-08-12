from graph_tool import GraphView
import numpy as np
import pandas as pd

def repeated_predicate_lists( D, edge_labels=np.empty(0), stats=dict(), print_stats=False, return_collected=True ):
    """"""

    if edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # filter those vertices v | out-degree(v) > 0
    S = GraphView( D, vfilt=D.get_out_degrees( D.get_vertices() ) )

    # .. is defined as the ratio of repeated predicate lists from the total lists in the graph G
    df = pd.DataFrame( 
        data=list( zip( D.get_edges()[:,0], edge_labels ) ), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0)[1].apply(tuple).apply(hash).to_frame().reset_index()

    if return_collected:
        df = df.groupby(1).count()[0]

        if print_stats:
            print( "(Eq.17) ratio of repeated predicate lists r_L(G): %f" % (1 - ( df.size / S.num_vertices() )) )
            print( "(Eq.18/19) predicate list degree deg_{PL}(G). max: %f, mean: %f" % ( df.max(), df.mean() ) )

        stats['repeated_predicate_lists'] = 1 - ( df.size / S.num_vertices() )
        stats['max_predicate_list_degree'], stats['mean_predicate_list_degree'] = df.max(), df.mean()

    return df

def collect_repeated_predicate_lists( S_G_s, edge_labels, df=pd.DataFrame(), stats=dict(), print_stats=False ):
    """"""
    if df is None:
        df = pd.DataFrame()

    df = df.append( repeated_predicate_lists( S_G_s, edge_labels, stats, print_stats, False ), ignore_index=True )

    return df

def reduce_repeated_predicate_lists( df, G, stats={} ):
    """"""
    df = df.groupby(1).count()[0]

    stats['repeated_predicate_lists'] = 1 - ( df.size / G.num_vertices() )
    stats['max_predicate_list_degree'], stats['mean_predicate_list_degree'] = df.max(), df.mean()

METRICS     = [ repeated_predicate_lists ]
METRICS_SET = { 'PREDICATE_LISTS': METRICS }
LABELS      = [ 'repeated_predicate_lists', 'max_predicate_list_degree', 'mean_predicate_list_degree' ]
