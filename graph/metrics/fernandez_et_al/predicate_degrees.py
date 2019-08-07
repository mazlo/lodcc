import numpy as np

def predicate_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # number of triples of graph G, in which p occurs as predicate
    _, l = np.unique( edge_labels, return_counts=True )

    if print_stats:
        print( "(Eq.9) predicate degree deg_P(p). max: %s, mean: %f" % ( np.max(l), np.mean(l) ) )

    stats['max_predicate_degree'], stats['mean_predicate_degree'] = np.max(l), np.mean(l)

def predicate_in_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of different subjects of G with which p is related as a predicate
    df = pd.DataFrame( 
        data=list( zip (preds, D.get_edges()[:,0] ) ), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.10) predicate in-degree deg^{+}_P(p). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_predicate_in_degree'], stats['mean_predicate_in_degree'] = df.max(), df.mean()

def predicate_out_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = [ D.ep.c0[p] for p in D.get_edges() ]

    # the number of different objects of G with which p is related as a predicate
     df = pd.DataFrame( 
        data=list( zip (preds, D.get_edges()[:,1] ) ), 
        index=np.arange( 0, D.get_edges().shape[0] ), 
        columns=np.arange( 0, D.get_edges().shape[1] ) )

    df = df.groupby(0).nunique()[1]

    if print_stats:
        print( "(Eq.11) predicate out-degree deg^{-}_P(p). max: %s, mean: %f" % ( df.max(), df.mean() ) )

    stats['max_predicate_out_degree'], stats['mean_predicate_out_degree'] = df.max(), df.mean()

METRICS = [ predicate_degree, predicate_in_degree, predicate_out_degree ]
LABELS  = [ 'max_predicate_degree', 'mean_predicate_degree', 'max_predicate_in_degree', 'mean_predicate_in_degree', 'max_predicate_out_degree', 'mean_predicate_out_degree' ]
