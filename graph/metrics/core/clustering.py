
def f_avg_shortest_path( U, stats, sem ):
    # can I?
    with sem:
        stats['avg_shortest_path']=nx.average_shortest_path_length(U)
        log.debug( 'done avg_shortest_path' )

def f_global_clustering( U, stats ):
    """"""

    if 'global_clustering' in args['skip_features'] or not 'global_clustering' in args['features']:
        log.debug( 'Skipping global_clustering' )
        return

    stats['global_clustering']=global_clustering(U)[0]
    log.debug( 'done global_clustering' )

def f_avg_clustering( D, stats ):
    """"""
    
    if 'local_clustering' in args['skip_features'] or not 'local_clustering' in args['features']:
        log.debug( 'Skipping avg_clustering' )
        return

    stats['avg_clustering']=vertex_average(D, local_clustering(D))[0]
    log.debug( 'done avg_clustering' )
