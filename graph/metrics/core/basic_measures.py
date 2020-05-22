import logging

from graph_tool import GraphView
from graph_tool.stats import label_parallel_edges

log = logging.getLogger( __name__ )

def fs_digraph_using_basic_properties( D, stats, options={ 'features': None } ):
    """"""

    # at least one of these features needed to continue
    if len([f for f in ['degree','parallel_edges','fill'] if f in options['features']]) == 0:
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
    if 'degree' in options['features']:
        stats['avg_degree']=float( 2*num_edges ) / num_vertices
        log.debug( 'done avg_degree' )
    
    # feature: fill_overall
    if 'fill' in options['features']:
        stats['fill_overall']=float( num_edges ) / ( num_vertices * num_vertices )
        log.debug( 'done fill_overall' )

    if 'parallel_edges' in options['features'] or 'fill' in options['features']:
        eprop = label_parallel_edges( D, mark_only=True )
        PE = GraphView( D, efilt=eprop )
        num_edges_PE = PE.num_edges()

        stats['m_unique']=num_edges - num_edges_PE

        # feature: parallel_edges
        if 'parallel_edges' in options['features']:
            stats['parallel_edges']=num_edges_PE
            log.debug( 'done parallel_edges' )

        # feature: fill
        if 'fill' in options['features']:
            stats['fill']=float( num_edges - num_edges_PE ) / ( num_vertices * num_vertices )
            log.debug( 'done fill' )
