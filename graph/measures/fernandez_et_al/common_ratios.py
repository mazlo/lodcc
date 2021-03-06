import numpy as np

def subject_object_ratio( D, edge_labels=None, stats=dict(), print_stats=False ):
    """"""

    # the number of elements acting both as subject and objects among all subjects and objects
    nom_intersection = set( D.get_edges()[:,0] ) & set( D.get_edges()[:,1] )
    denom_union = set( D.get_edges()[:,0] ) | set( D.get_edges()[:,1] )

    if print_stats:
        print( "(Eq.12) subject-object ratio \\alpha_{s-o}(G): %f" % ( float(len(nom_intersection)) / len(denom_union) ) )
    
    stats['subject_object_ratio'] = float(len(nom_intersection)) / len(denom_union)

def subject_predicate_ratio( D, edge_labels=np.empty(0), stats=dict() ):
    """"""

    # TODO because this is a costly computation

def predicate_object_ratio( D, edge_labels=np.empty(0), stats=dict() ):
    """"""

    # TODO because this is a costly computation

METRICS = [ subject_object_ratio ]
LABELS  = [ 'subject_object_ratio' ]