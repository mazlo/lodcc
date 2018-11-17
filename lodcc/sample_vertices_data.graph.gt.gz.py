from graph_tool import *

def xyz( dataset, k ):
    """creates a sampled sub graph from dataset with k vertices and corresponding edges"""

    # dataset e.g. 'dumps/education-data-gov-uk/data.graph.gt.gz'
    D = load_graph( dataset )
    
    vfilt   = D.new_vertex_property( 'bool' )
    v       = D.get_vertices()
    v_rand  = np.random.choice( v, size=int( len(v)*k ), replace=False )
    
    for e in v_rand:
        vfilt[e] = True
        
        D_sub   = GraphView( D, vfilt=vfilt )
        
        '%s/data.edgelist.csv' % sample_dir, 'w'
        
        graph_gt = '/'.join( [os.path.dirname( dataset ), 'data.graph.%s.gt.gz' % k] )
        D_sub.save( graph_gt )

# TODO ZL
