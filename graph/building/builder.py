import logging
import numpy as np
np.warnings.filterwarnings('ignore')
import os
import re

try:
    from graph_tool import Graph, GraphView, load_graph, load_graph_from_csv
except:
    print( 'graph_tool module could not be imported' )

log = logging.getLogger( __name__ )

def load_graph_from_edgelist( dataset, stats={}, options={} ):
    """"""

    edgelist, graph_gt = dataset['path_edgelist'], dataset['path_graph_gt']

    D=None

    # prefer graph_gt file
    if 'reconstruct_graph' in options and not options['reconstruct_graph'] and \
        graph_gt and os.path.isfile( graph_gt ):
        log.info( 'Constructing DiGraph from gt.xz' )
        D=load_graph( graph_gt )
    
    elif edgelist and os.path.isfile( edgelist ):
        log.info( 'Constructing DiGraph from edgelist' )

        if 'dict_hashed' in options and options['dict_hashed']:
            D=load_graph_from_csv( edgelist, directed=True, string_vals=False, hashed=False, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
        else:
            D=load_graph_from_csv( edgelist, directed=True, string_vals=True, hashed=True, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
    
    else:
        log.error( 'edgelist or graph_gt file to read graph from does not exist' )
        return None

    # dump graph after reading if required
    if D and 'dump_graph' in options and options['dump_graph']:
        log.info( 'Dumping graph..' )

        prefix = re.split( '.edgelist.csv', os.path.basename( edgelist ) )
        if prefix[0] != 'data':
            prefix = prefix[0]
        else:
            prefix = 'data'

        graph_gt = '/'.join( [os.path.dirname( edgelist ), '%s.graph.gt.gz' % prefix] )
        D.save( graph_gt )
        stats['path_graph_gt'] = graph_gt

        # thats it here
        if 'print_stats' in options and not options['print_stats'] and \
            'from_file' in options and not options['from_file']:
            save_stats( dataset, stats )

    # check if subgraph is required
    if D and 'sample_vertices' in options and options['sample_vertices']:
        k = options['sample_size']
        
        vfilt   = D.new_vertex_property( 'bool' )
        v       = D.get_vertices()
        v_rand  = np.random.choice( v, size=int( len(v)*k ), replace=False )

        log.info( 'Sampling vertices ...')

        for e in v_rand:
            vfilt[e] = True
        
        return GraphView( D, vfilt=vfilt )

    return D
