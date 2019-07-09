from graph_tool import GraphView
import numpy as np
import pandas as pd

def repeated_predicate_lists( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    S = GraphView( D, vfilt=D.get_edges()[:,0] )

    # .. is defined as the ratio of repeated predicate lists from the total lists in the graph G
    df = pd.DataFrame( 
        data=list(zip( D.get_edges()[:,0], edge_labels )), 
        index=np.arange(0, D.get_edges().shape[0]), 
        columns=np.arange(0, D.get_edges().shape[1]-1) )

    df = df.groupby(0)[1].apply(list).apply(tuple).apply(hash).to_frame().reset_index()
    df = df.groupby(1).count()[0]

    if print_stats:
        print( "(Eq.17) ratio of repeated predicate lists r_L(G): %f" % (1 - ( df.size / S.num_vertices() )) )
        print( "(Eq.18/19) predicate list degree deg_{PL}(G). max: %f, mean: %f" % ( df.max(), df.mean() ) )

    stats['repeated_predicate_lists'] = 1 - ( df.size / S.num_vertices() )
    stats['max_predicate_list_degree'], stats['mean_predicate_list_degree'] = df.max(), df.mean()

METRICS = [ repeated_predicate_lists ]
LABELS  = [ 'repeated_predicate_lists', 'max_predicate_list_degree', 'mean_predicate_list_degree' ]