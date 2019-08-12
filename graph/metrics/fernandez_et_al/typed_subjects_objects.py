from graph_tool import GraphView
import numpy as np

def number_of_classes( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """counts the number of different classes"""

    if edge_labels.size == 0:
        edge_labels = np.array( [ D.ep.c0[p] for p in D.get_edges() ] )

    # ae98476863dc6ec5 = http://www.w3.org/1999/02/22-rdf-syntax-ns#type
    C_G = GraphView( D, efilt=edge_labels == 'ae98476863dc6ec5' )
    C_G = np.unique( C_G.get_edges()[:,1] )

    if print_stats:
        print( "number of different classes C_G: %s" % C_G.size )

    stats['distinct_classes'] = C_G.size

def ratio_of_typed_subjects( D, edge_labels=np.empty(0), stats=dict(), print_stats=False ):
    """
        (1) number of all different typed subjects
        (2) ratio of typed subjects
    """

    if edge_labels.size == 0:
        edge_labels = np.array( [ D.ep.c0[p] for p in D.get_edges() ] )

    # ae98476863dc6ec5 = http://www.w3.org/1999/02/22-rdf-syntax-ns#type
    S_C_G = GraphView( D, efilt=edge_labels == 'ae98476863dc6ec5' )
    S_C_G = np.unique( S_C_G.get_edges()[:,0] )

    if print_stats:
        print( "number of different typed subjects S^{C}_G: %s" % S_C_G.size )

    S_G = GraphView( D, vfilt=D.get_out_degrees( D.get_vertices() ) )

    if print_stats:
        print( "ratio of typed subjects r_T(G): %s" % ( float(S_C_G.size)/S_G.num_vertices() ) )

    stats['typed_subjects'], stats['ratio_of_typed_subjects'] = S_C_G.size, ( float(S_C_G.size)/S_G.num_vertices() )

METRICS = [ number_of_classes, ratio_of_typed_subjects ]
LABELS  = [ 'distinct_classes', 'typed_subjects', 'ratio_of_typed_subjects' ]
