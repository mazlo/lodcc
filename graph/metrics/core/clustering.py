import logging 

from graph_tool.clustering import global_clustering, local_clustering
from graph_tool.stats import vertex_average

log = logging.getLogger( __name__ )

def f_global_clustering( U, stats, options={ 'features': [], 'skip_features': [] } ):
    """"""

    if not 'global_clustering' in options['features'] or ('skip_features' in options and 'global_clustering' in options['skip_features']):
        log.debug( 'Skipping global_clustering' )
        return

    stats['global_clustering']=global_clustering(U)[0]
    log.debug( 'done global_clustering' )

def f_local_clustering( D, stats, options={ 'features': [], 'skip_features': [] } ):
    """"""
    
    if not 'local_clustering' in options['features'] or ('skip_features' in options and 'local_clustering' in options['skip_features']):
        log.debug( 'Skipping local_clustering' )
        return

    stats['avg_clustering']=vertex_average(D, local_clustering(D))[0]
    log.debug( 'done local_clustering' )
