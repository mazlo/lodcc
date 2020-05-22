import logging
import threading

log = logging.getLogger( __name__ )
lock = threading.Lock()

def fs_digraph_using_degree( D, stats, options={ 'features': [], 'skip_features': [] } ):
    """"""

    # compute once
    degree_list = D.degree_property_map( 'total' ).a

    # feature: max_(in|out)degree
    # feature: (in|out)_degree_centrality
    if 'degree' in args['features']:

        v_max = (0, None)
        v_max_in = (0, None)
        v_max_out = (0, None)

        sum_degrees = 0.0
        sum_in_degrees = 0.0
        sum_out_degrees = 0.0

        # max_(in|out)degree are computed that way because we want also the node's name
        for v in D.vertices():
            v_in_degree = v.in_degree()
            v_out_degree = v.out_degree()

            v_degree = v_in_degree + v_out_degree
            # for max_degree, max_degree_vertex
            v_max = ( v_degree,v ) if v_degree >= v_max[0] else v_max
            # for max_in_degree, max_in_degree_vertex
            v_max_in = ( v_in_degree,v ) if v_in_degree >= v_max_in[0] else v_max_in
            # for max_out_degree, max_out_degree_vertex
            v_max_out = ( v_out_degree,v ) if v_out_degree >= v_max_out[0] else v_max_out

            sum_degrees += v_degree
            sum_in_degrees += v_in_degree
            sum_out_degrees += v_out_degree

        stats['max_degree'], stats['max_degree_vertex'] = v_max[0], str( D.vertex_properties['name'][v_max[1]] )
        stats['max_in_degree'], stats['max_in_degree_vertex'] = v_max_in[0], str( D.vertex_properties['name'][v_max_in[1]] )
        stats['max_out_degree'], stats['max_out_degree_vertex'] = v_max_out[0], str( D.vertex_properties['name'][v_max_out[1]] )

        log.debug( 'done degree' )

        # feature: degree_centrality
        num_vertices = stats['n']
        s = 1.0 / ( num_vertices - 1 )

        stats['avg_degree_centrality']=(sum_degrees*s) / num_vertices
        stats['avg_in_degree_centrality']=(sum_in_degrees*s) / num_vertices
        stats['avg_out_degree_centrality']=(sum_out_degrees*s) / num_vertices

        stats['max_degree_centrality']=v_max[0]*s
        stats['max_in_degree_centrality']=v_max_in[0]*s
        stats['max_out_degree_centrality']=v_max_out[0]*s

        # stats['centralization_in_degree'] = (v_max_in[0]-(D.get_in_degrees( D.get_vertices() ))).sum() / ( ( num_vertices-1 )*(num_vertices-2))
        # stats['centralization_out_degree'] = (v_max_out[0]-(D.get_out_degrees( D.get_vertices() ))).sum() / ( ( num_vertices-1 )*(num_vertices-2))


        # feature: standard deviation
        stddev_in_degree = D.get_in_degrees( D.get_vertices() ).std()
        stats['stddev_in_degree'] = stddev_in_degree
        stats['coefficient_variation_in_degree'] = ( stddev_in_degree / ( sum_in_degrees / num_vertices ) ) * 100
        stddev_out_degree = D.get_out_degrees( D.get_vertices() ).std()
        stats['stddev_out_degree'] = stddev_out_degree
        stats['coefficient_variation_out_degree'] = ( stddev_out_degree / ( sum_out_degrees / num_vertices ) ) * 100

        stats['var_in_degree'] = D.get_in_degrees( D.get_vertices() ).var()
        stats['var_out_degree'] = D.get_out_degrees( D.get_vertices() ).var()

        log.debug( 'done standard deviation and variance' )

    if 'gini' in args['features']:
        gini_coeff = gini( degree_list )
        stats['gini_coefficient'] = gini_coeff

        gini_coeff_in_degree = gini( D.get_in_degrees( D.get_vertices() ) )
        stats['gini_coefficient_in_degree'] = gini_coeff_in_degree
    
        gini_coeff_out_degree = gini( D.get_out_degrees( D.get_vertices() ) )
        stats['gini_coefficient_out_degree'] = gini_coeff_out_degree

    # feature: h_index_u
    if 'h_index' in args['features']:
        degree_list[::-1].sort()
    
        h = 0
        for x in degree_list:
            if x >= h + 1:
                h += 1
            else:
                break

        stats['h_index_u']=h
        log.debug( 'done h_index_u' )

    # feature: p_law_exponent
    if 'powerlaw' in args['features']:
        fit = powerlaw.Fit( degree_list )
        
        stats['powerlaw_exponent_degree'] = float( fit.power_law.alpha )
        stats['powerlaw_exponent_degree_dmin'] = float( fit.power_law.xmin )
        log.debug( 'done powerlaw_exponent' )

    # plot degree distribution
    if not 'plots' in args['skip_features'] and 'plots' in args['features']:
        degree_counted = collections.Counter( degree_list )
        degree, counted = zip( *degree_counted.items() )

        lock.acquire()

        fig, ax = plt.subplots()
        plt.plot( degree, counted )

        plt.title( 'Degree Histogram' )
        plt.ylabel( 'Frequency' )
        plt.xlabel( 'Degree' )

        ax.set_xticklabels( degree )

        ax.set_xscale( 'log' )
        ax.set_yscale( 'log' )

        plt.tight_layout()
        plt.savefig( '/'.join( [os.path.dirname( stats['path_edgelist'] ), 'distribution_degree.pdf'] ) )
        degree_counted = collections.Counter( degree_list )
        log.debug( 'done plotting degree distribution' )

        lock.release()

def fs_digraph_using_indegree( D, stats, options={ 'features': [], 'skip_features': [] } ):
    """"""

    # compute once
    degree_list = D.get_in_degrees( D.get_vertices() )

    # feature: h_index_d
    if 'h_index' in args['features']:
        degree_list[::-1].sort()
    
        h = 0
        for x in degree_list:
            if x >= h + 1:
                h += 1
            else:
                break
    
        stats['h_index_d']=h
        log.debug( 'done h_index_d' )

    # feature: p_law_exponent
    if 'powerlaw' in args['features']:
        fit = powerlaw.Fit( degree_list )
        
        stats['powerlaw_exponent_in_degree'] = float( fit.power_law.alpha )
        stats['powerlaw_exponent_in_degree_dmin'] = float( fit.power_law.xmin )
        log.debug( 'done powerlaw_exponent' )

    # plot degree distribution
    if not 'plots' in args['skip_features'] and 'plots' in args['features']:
        degree_counted = collections.Counter( degree_list )
        degree, counted = zip( *degree_counted.items() )

        lock.acquire()

        fig, ax = plt.subplots()
        plt.plot( degree, counted )

        plt.title( 'In-Degree Histogram' )
        plt.ylabel( 'Frequency' )
        plt.xlabel( 'In-Degree' )

        ax.set_xticklabels( degree )

        ax.set_xscale( 'log' )
        ax.set_yscale( 'log' )

        plt.tight_layout()
        plt.savefig( '/'.join( [os.path.dirname( stats['path_edgelist'] ), 'distribution_in-degree.pdf'] ) )
        log.debug( 'done plotting in-degree distribution' )

        lock.release()
