import numpy as np

def predicate_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    # number of triples of graph G, in which p occurs as predicate
    _, l = np.unique( edge_labels, return_counts=True )

    if print_stats:
        print( "(Eq.9) predicate degree deg_P(p). max: %s, mean: %f" % ( np.max(l), np.mean(l) ) )

    stats['max_predicate_degree'], stats['mean_predicate_degree'] = np.max(l), np.mean(l)

def predicate_in_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    # the number of different subjects of G with which p is related as a predicate
    l = list( zip( edge_labels, D.get_edges()[:,0] ) )
    _, l = np.unique( l, return_counts=True, axis=0 )

    if print_stats:
        print( "(Eq.10) predicate in-degree deg^{+}_P(p). max: %s, mean: %f" % ( np.max(l), np.mean(l) ) )

    stats['max_predicate_in_degree'], stats['mean_predicate_in_degree'] = np.max(l), np.mean(l)

def predicate_out_degree( D, stats, edge_labels=np.empty(0), print_stats=False ):
    """"""

    if edge_labels.size == 0:
        edge_labels = D.ep.c0.get_2d_array([0])[0]

    l = list( zip( edge_labels, D.get_edges()[:,1] ) )
    _, l = np.unique( l, return_counts=True, axis=0 )

    if print_stats:
        print( "(Eq.11) predicate out-degree deg^{-}_P(p). max: %s, mean: %f" % ( np.max(l), np.mean(l) ) )

    stats['max_predicate_out_degree'], stats['mean_predicate_out_degree'] = np.max(l), np.mean(l)

METRICS = [ predicate_degree, predicate_in_degree, predicate_out_degree ]
LABELS  = [ 'max_predicate_degree', 'mean_predicate_degree', 'max_predicate_in_degree', 'mean_predicate_in_degree', 'max_predicate_out_degree', 'mean_predicate_out_degree' ]