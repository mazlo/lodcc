import logging
import threading

log = logging.getLogger( __name__ )
lock = threading.Lock()

def f_centralization( D, stats ):
    """"""

    if not 'centralization' in args['features']:
        return

    D_copied = D.copy()
    D = None

    remove_parallel_edges( D_copied )

    degree_list = D_copied.degree_property_map( 'total' ).a
    max_degree  = degree_list.max()

    stats['centralization_degree'] = float((max_degree-degree_list).sum()) / ( ( degree_list.size-1 )*(degree_list.size-2))
    
    # stats['centralization_in_degree'] = (v_max_in[0]-(D.get_in_degrees( D.get_vertices() ))).sum() / ( ( num_vertices-1 )*(num_vertices-2))
    # stats['centralization_out_degree'] = (v_max_out[0]-(D.get_out_degrees( D.get_vertices() ))).sum() / ( ( num_vertices-1 )*(num_vertices-2))

    log.debug( 'done centrality measures' )

def f_eigenvector_centrality( D, stats ):
    """"""

    if 'eigenvector_centrality' not in args['features']:
        log.debug( 'Skipping eigenvector_centrality' )
        return

    eigenvector_list = eigenvector(D)[1].get_array()
        
    # info: vertex with largest eigenvector value
    ev_list_idx=zip( eigenvector_list, D.vertex_index )
    largest_ev_vertex=reduce( (lambda new_tpl, last_tpl: new_tpl if new_tpl[0] >= last_tpl[0] else last_tpl), ev_list_idx )
    stats['max_eigenvector_vertex']=D.vertex_properties['name'][largest_ev_vertex[1]]
    log.debug( 'done max_eigenvector_vertex' )

    eigenvector_list[::-1].sort()

    # plot degree distribution
    values_counted = collections.Counter( eigenvector_list )
    values, counted = zip( *values_counted.items() )
        
    lock.acquire()

    fig, ax = plt.subplots()
    plt.plot( values, counted )

    plt.title( 'Eigenvector-Centrality Histogram' )
    plt.ylabel( 'Frequency' )
    plt.xlabel( 'Eigenvector-Centrality Value' )

    ax.set_xticklabels( values )

    ax.set_xscale( 'log' )
    ax.set_yscale( 'log' )

    plt.tight_layout()
    plt.savefig( '/'.join( [os.path.dirname( stats['path_edgelist'] ), 'distribution_eigenvector-centrality.pdf'] ) )
    log.debug( 'done plotting eigenvector_centrality' )

    lock.release()
        
def f_pagerank( D, stats ):
    """"""

    if 'pagerank' not in args['features']:
        log.debug( 'Skipping pagerank' )
        return

    pagerank_list = pagerank(D).get_array()

    pr_max = (0.0, 0)
    idx = 0

    # iterate and collect max value and idx
    for pr_val in pagerank_list:
        pr_max = ( pr_val, idx ) if pr_val >= pr_max[0] else pr_max
        idx += 1

    stats['max_pagerank'], stats['max_pagerank_vertex'] = pr_max[0], str( D.vertex_properties['name'][pr_max[1]] )

    if not 'plots' in args['skip_features'] and 'plots' in args['features']:
        pagerank_list[::-1].sort()

        # plot degree distribution
        values_counted = collections.Counter( pagerank_list )
        values, counted = zip( *values_counted.items() )
    
        lock.acquire()

        fig, ax = plt.subplots()
        plt.plot( values, counted )

        plt.title( 'PageRank Histogram' )
        plt.ylabel( 'Frequency' )
        plt.xlabel( 'PageRank Value' )

        ax.set_xticklabels( values )

        ax.set_xscale( 'log' )
        ax.set_yscale( 'log' )

        plt.tight_layout()
        plt.savefig( '/'.join( [os.path.dirname( stats['path_edgelist'] ), 'distribution_pagerank.pdf'] ) )
        log.debug( 'done plotting pagerank distribution' )

        lock.release()
