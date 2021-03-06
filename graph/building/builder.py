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

def dump_graph( D, edgelist_path, options={} ):
    """"""

    # dump graph after reading if required
    if D and 'dump_graph' in options and options['dump_graph']:
        log.info( 'Dumping graph..' )

        prefix = re.split( '.edgelist.csv', os.path.basename( edgelist_path ) )
        if prefix[0] != 'data':
            prefix = prefix[0]
        else:
            prefix = 'data'

        graph_gt_path = '/'.join( [os.path.dirname( edgelist_path ), '%s.graph.gt.gz' % prefix] )
        D.save( graph_gt_path )
        
def load_graph_from_edgelist( dataset, options={} ):
    """"""

    edgelist, graph_gt = dataset['path_edgelist'], dataset['path_graph_gt']

    D=None

    # prefer graph_gt file
    if (not 'reconstruct_graph' in options or not options['reconstruct_graph']) and \
        (graph_gt and os.path.isfile( graph_gt )):
        log.info( 'Constructing DiGraph from gt.xz' )
        D=load_graph( graph_gt )
    
    elif edgelist and os.path.isfile( edgelist ):
        log.info( 'Constructing DiGraph from edgelist' )

        if 'dict_hashed' in options and options['dict_hashed']:
            D=load_graph_from_csv( edgelist, directed=True, hashed=False, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
        else:
            D=load_graph_from_csv( edgelist, directed=True, hashed=True, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
    
        # check if graph should be dumped
        dump_graph( D, edgelist, options )
    else:
        log.error( 'edgelist or graph_gt file to read graph from does not exist' )
        return None

    return D
