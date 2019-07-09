from graph_tool import GraphView
import numpy as np
import pandas as pd

def repeated_predicate_lists( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    S = GraphView( D, vfilt=D.get_edges()[:,0] )

    # .. is defined as the ratio of repeated predicate lists from the total lists in the graph G
    l = zip( D.get_edges()[:,0], edge_labels )

    df = pd.DataFrame( 
        data=list(l), 
        index=np.arange(0, D.get_edges().shape[0]), 
        columns=np.arange(0, D.get_edges().shape[1]-1) )

    L_G = df.groupby(0)[1].apply(list).apply(tuple).apply(hash).to_frame().reset_index()
    L_G = L_G.groupby(1).count()

        print( "(Eq.17) ratio of repeated predicate lists r_L(G): %f" % (1 - ( L_G.size / S.num_vertices() )) )
        print( "(Eq.18/19) predicate list degree deg_{PL}(G). max: %f, mean: %f" % ( L_G.max(), L_G.mean() ) )

METRICS = [ repeated_predicate_lists ]
LABELS  = [ 'repeated_predicate_lists', 'max_predicate_list_degree', 'mean_predicate_list_degree' ]