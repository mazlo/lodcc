def fs_digraph_using_basic_properties( D, stats ):
    """"""

    # at least one of these features needed to continue
    if len([f for f in ['degree','parallel_edges','fill'] if f in args['features']]) == 0:
        return

    # feature: order
    num_vertices = D.num_vertices()
    log.debug( 'done order' )

    # feature: size
    num_edges = D.num_edges()
    log.debug( 'done size' )

    stats['n']=num_vertices
    stats['m']=num_edges

    # feature: avg_degree
    if 'degree' in args['features']:
        stats['avg_degree']=float( 2*num_edges ) / num_vertices
        log.debug( 'done avg_degree' )
    
    # feature: fill_overall
    if 'fill' in args['features']:
        stats['fill_overall']=float( num_edges ) / ( num_vertices * num_vertices )
        log.debug( 'done fill_overall' )

    if 'parallel_edges' in args['features'] or 'fill' in args['features']:
        eprop = label_parallel_edges( D, mark_only=True )
        PE = GraphView( D, efilt=eprop )
        num_edges_PE = PE.num_edges()

        stats['m_unique']=num_edges - num_edges_PE

        # feature: parallel_edges
        if 'parallel_edges' in args['features']:
            stats['parallel_edges']=num_edges_PE
            log.debug( 'done parallel_edges' )

        # feature: fill
        if 'fill' in args['features']:
            stats['fill']=float( num_edges - num_edges_PE ) / ( num_vertices * num_vertices )
            log.debug( 'done fill' )
