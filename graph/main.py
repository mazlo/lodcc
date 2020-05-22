# DURL=`psq -U zlochms -d cloudstats -c "SELECT url FROM stats WHERE domain='Cross_domain' AND title LIKE '%Museum%'" -t -A`
# curl -L "$DURL/datapackage.json" -o datapackage.json

# -----------------

# preparation
#
# + import tsv file into database
# + obtain download urls from datahub.io (extends database table)

import re
import os
import argparse
import logging as log
import numpy as np
import threading
import sys

try:
    import psycopg2
    import psycopg2.extras
except:
    print( 'psycogp2 could not be found' )
try:
    from graph.gini import gini
except:
    print( 'One of other lodcc modules could not be found. Make sure you have imported all requirements.' )

conn = None

def ensure_db_schema_complete( cur, table_name, attribute ):
    """ensure_db_schema_complete"""

    log.debug( 'Checking if column %s exists', attribute )
    cur.execute( "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_name = %s;", (table_name, attribute) )

    if cur.rowcount == 0:
        log.info( 'Creating missing attribute %s', attribute )
        cur.execute( "ALTER TABLE %s ADD COLUMN "+ attribute +" varchar;", (table_name,) )

    log.debug( 'Found %s-attribute', attribute )
    return attribute

def ensure_db_record_is_unique( cur, name, table_name, attribute, value ):
    """ensure_db_record_is_unique"""

    cur.execute( 'SELECT id FROM %s WHERE name = %s AND ('+ attribute +' IS NULL OR '+ attribute +' = %s)', (table_name, name, "") )

    if cur.rowcount != 0:
        # returns the id of the row to be updated
        return cur.fetchone()[0]
    else:
        # insert new row and return the id of the row to be updated
        log.info( 'Attribute %s not unique for "%s". Will create a new row.', attribute, name )
        cur.execute( 'INSERT INTO %s (id, name, '+ attribute +') VALUES (default, %s, %s) RETURNING id', (table_name, name, value) )

        return cur.fetchone()[0]

def save_value( cur, dataset_id, dataset_name, table_name, attribute, value, check=True ):
    """save_value"""

    ensure_db_schema_complete( cur, table_name, attribute )

    if check and not value:
        # TODO create warning message
        log.warn( 'No value for attribute '+ attribute +'. Cannot save' )
        return
    elif check:
        # returns the id of the row to be updated
        dataset_id = ensure_db_record_is_unique( cur, dataset_name, table_name, attribute, value )
    
    log.debug( 'Saving value "%s" for attribute "%s" for "%s"', value, attribute, dataset_name )
    cur.execute( 'UPDATE %s SET '+ attribute +' = %s WHERE id = %s;', (table_name, value, dataset_id) )

# -----------------

try:
    from graph_tool.all import *
except:
    print( 'graph_tool module could not be imported' )
import numpy as n
import powerlaw
n.warnings.filterwarnings('ignore')

import collections
try:
    import matplotlib.pyplot as plt
except:
    print( 'matplotlib.pyplot module could not be imported' )

lock = threading.Lock()

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

def fs_digraph_using_degree( D, stats ):
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

def fs_digraph_using_indegree( D, stats ):
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

def f_reciprocity( D, stats ):
    """"""

    if 'reciprocity' in args['features']:
        stats['reciprocity']=edge_reciprocity(D)
        log.debug( 'done reciprocity' )

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

def save_stats( dataset, stats ):
    """"""

    # e.g. avg_degree=%(avg_degree)s, max_degree=%(max_degree)s, ..
    cols = ', '.join( map( lambda d: d +'=%('+ d +')s', stats ) )

    sql=('UPDATE %s SET ' % args['db_tbname']) + cols +' WHERE id=%(id)s'
    stats['id']=dataset[0]

    cur = conn.cursor()
    cur.execute( sql, stats )
    conn.commit()
    cur.close()

    log.debug( 'done saving results' )

def f_pseudo_diameter( D, stats ):
    """"""

    LC = label_largest_component(D)
    LCD = GraphView( D, vfilt=LC )

    if 'diameter' in args['features']:
        if LCD.num_vertices() == 0 or LCD.num_vertices() == 1:
            # if largest component does practically not exist, use the whole graph
            dist, ends = pseudo_diameter(D)
        else:
            dist, ends = pseudo_diameter(LCD)

        stats['pseudo_diameter']=dist
        # D may be used in both cases
        stats['pseudo_diameter_src_vertex']=D.vertex_properties['name'][ends[0]]
        stats['pseudo_diameter_trg_vertex']=D.vertex_properties['name'][ends[1]]
        log.debug( 'done pseudo_diameter' )

def fs_digraph_start_job( dataset, D, stats ):
    """"""

    features = [ 
        # fs = feature set
        fs_digraph_using_basic_properties,
        fs_digraph_using_degree, fs_digraph_using_indegree,
        f_centralization,
        f_reciprocity,
        f_pseudo_diameter,
        f_avg_clustering,
        f_pagerank, 
        f_eigenvector_centrality,
    ]

    for ftr in features:
        ftr( D, stats )

        if not args['print_stats'] and not args['from_file']:
            save_stats( dataset, stats )

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

def fs_ugraph_start_job( dataset, U, stats ):
    """"""

    features = [ 
        # fs = feature set
        f_global_clustering, #f_avg_clustering, 
        # f_avg_shortest_path, 
    ]

    for ftr in features:
        ftr( U, stats )

        if not args['print_stats'] and not args['from_file']:
            save_stats( dataset, stats )

def load_graph_from_edgelist( dataset, stats ):
    """"""

    edgelist, graph_gt = dataset['path_edgelist'], dataset['path_graph_gt']

    D=None

    # prefer graph_gt file
    if not args['reconstruct_graph'] and graph_gt and os.path.isfile( graph_gt ):
        log.info( 'Constructing DiGraph from gt.xz' )
        D=load_graph( graph_gt )
    
    elif edgelist and os.path.isfile( edgelist ):
        log.info( 'Constructing DiGraph from edgelist' )

        if args['dict_hashed']:
            D=load_graph_from_csv( edgelist, directed=True, string_vals=False, hashed=False, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
        else:
            D=load_graph_from_csv( edgelist, directed=True, string_vals=True, hashed=True, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
    
    else:
        log.error( 'edgelist or graph_gt file to read graph from does not exist' )
        return None

    # dump graph after reading if required
    if D and args['dump_graph']:
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
        if not args['print_stats'] and not args['from_file']:
            save_stats( dataset, stats )

    # check if subgraph is required
    if D and args['sample_vertices']:
        k = args['sample_size']
        
        vfilt   = D.new_vertex_property( 'bool' )
        v       = D.get_vertices()
        v_rand  = np.random.choice( v, size=int( len(v)*k ), replace=False )

        log.info( 'Sampling vertices ...')

        for e in v_rand:
            vfilt[e] = True
        
        return GraphView( D, vfilt=vfilt )

    return D

def graph_analyze( dataset, stats ):
    """"""
   
    D=load_graph_from_edgelist( dataset, stats )

    if not D:
        log.error( 'Could not instantiate graph, None' )
        return

    log.info( 'Computing feature set DiGraph' )
    fs_digraph_start_job( dataset, D, stats )
    
    D.set_directed(False)
    log.info( 'Computing feature set UGraph' )
    fs_ugraph_start_job( dataset, D, stats )
    
    # slow
    #stats['k_core(U)']=nx.k_core(U)
    #stats['radius(U)']=nx.radius(U)
    
    return stats

def build_graph_analyse( dataset, threads_openmp=7 ):
    """"""

    # before starting off: limit the number of threads a graph_tool job may acquire
    graph_tool.openmp_set_num_threads( threads_openmp )

    # init stats
    
    stats = dict( (attr, dataset[attr]) for attr in ['path_edgelist','path_graph_gt'] )
    graph_analyze( dataset, stats )

    if args['print_stats']:
        if args['from_file']:
            print( ', '.join( [ key for key in stats.keys() ] ) )
            print( ', '.join( [ str(stats[key]) for key in stats.keys() ] ) )
        else:
            print( stats )

# real job
def job_start_build_graph( dataset, sem, threads_openmp=7 ):
    """job_start_build_graph"""

    # let's go
    with sem:
        log.info( 'Let''s go' )
        log.debug( dataset )

        # - build_graph_analyse
        build_graph_analyse( dataset, threads_openmp )

        # - job_cleanup

        log.info( 'Done' ) 

def build_graph( datasets, no_of_threads=1, threads_openmp=7 ):
    """"""

    if len( datasets ) == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )
    threads = []

    for dataset in datasets:
        
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start_build_graph, name = dataset['name'], args = ( dataset, sem, threads_openmp ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )
    actions = parser.add_mutually_exclusive_group( required = True )

    actions.add_argument( '--build-graph', '-pa', action = "store_true", help = '' )
    
    parser.add_argument( '--dry-run', '-d', action = "store_true", help = '' )

    parser.add_argument( '--use-datasets', '-du', nargs='*', help = '' )
    parser.add_argument( '--overwrite-dl', '-ddl', action = "store_true", help = 'If this argument is present, the program WILL NOT use data dumps which were already dowloaded, but download them again' )
    parser.add_argument( '--overwrite-nt', '-dnt', action = "store_true", help = 'If this argument is present, the program WILL NOT use ntriple files which were already transformed, but transform them again' )
    parser.add_argument( '--rm-original', '-dro', action = "store_true", help = 'If this argument is present, the program WILL REMOVE the original downloaded data dump file' )
    parser.add_argument( '--keep-edgelists', '-dke', action = "store_true", help = 'If this argument is present, the program WILL KEEP single edgelists which were generated. A data.edgelist.csv file will be generated nevertheless.' )
    
    group = parser.add_mutually_exclusive_group( required = True )
    group.add_argument( '--from-db', '-fdb', action = "store_true", help = '' )
    group.add_argument( '--from-file', '-ffl', action = "append", help = '', nargs = '*')

    parser.add_argument( '--log-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-info', '-li', action = "store_true", help = '' )
    parser.add_argument( '--log-file', '-lf', action = "store_true", help = '' )
    parser.add_argument( '--print-stats', '-lp', action= "store_true", help = '' )
    parser.add_argument( '--threads', '-pt', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

    # TODO add --sample-edges
    parser.add_argument( '--sample-vertices', '-gsv', action = "store_true", help = '' )
    parser.add_argument( '--sample-size', '-gss', required = False, type = float, default = 0.2, help = '' )

    # RE graph or feature computation
    parser.add_argument( '--dump-graph', '-gs', action = "store_true", help = '' )
    parser.add_argument( '--reconstruct-graph', '-gr', action = "store_true", help = '' )
    parser.add_argument( '--dict-hashed', '-gh', action = "store_true", help = '' )
    parser.add_argument( '--threads-openmp', '-gth', required = False, type = int, default = 7, help = 'Specify how many threads will be used for the graph analysis' )
    parser.add_argument( '--do-heavy-analysis', '-gfsh', action = "store_true", help = '' )
    parser.add_argument( '--features', '-gfs', nargs='*', required = False, default = list(), help = '' )
    parser.add_argument( '--skip-features', '-gsfs', nargs='*', required = False, default = list(), help = '' )
    
    # args is available globaly
    args = vars( parser.parse_args() ).copy()

    # configure logging
    if args['log_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    if args['log_file']:
        log.basicConfig( filename = 'lodcc.log', filemode='w', level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )
    else:
        log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    # read all properties in file into args-dict
    if args['from_db']:
        log.debug( 'Requested to read data from db' )

        if not os.path.isfile( 'db.properties' ):
            log.error( '--from-db given but no db.properties file found. please specify.' )
            sys.exit(0)
        else:
            with open( 'db.properties', 'rt' ) as f:
                args.update( dict( ( key.replace( '.', '_' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) ) )
    
    elif args['from_file']:
        log.debug( 'Requested to read data from file' )

    if args['from_db']:
        # connect to an existing database
        conn = psycopg2.connect( host=args['db_host'], dbname=args['db_dbname'], user=args['db_user'], password=args['db_password'] )
        conn.set_session( autocommit=True )

        try:
            cur = conn.cursor()
            cur.execute( "SELECT 1;" )
            result = cur.fetchall()
            cur.close()

            log.debug( 'Database ready to query execution' )
        except:
            log.error( 'Database not ready for query execution. Check db.properties. db error:\n %s', sys.exc_info()[0] )
            raise 

    # option 3
    if args['build_graph'] or args['dump_graph']:

        if args['from_db']:
            # respect --use-datasets argument
            if args['use_datasets']:
                names_query = '( ' + ' OR '.join( 'name = %s' for ds in args['use_datasets'] ) + ' )'
                names = tuple( args['use_datasets'] )
            else:
                names = 'all'

            if args['dry_run']:
                log.info( 'Running in dry-run mode' )

                # if not given explicitely above, shrink available datasets to one special
                if not args['use_datasets']:
                    names_query = 'name = %s'
                    names = tuple( ['museums-in-italy'] )

            log.debug( 'Configured datasets: '+ ', '.join( names ) )

            if 'names_query' in locals():
                sql = ('SELECT id,name,path_edgelist,path_graph_gt FROM %s WHERE ' % args['db_tbname']) + names_query +' AND (path_edgelist IS NOT NULL OR path_graph_gt IS NOT NULL) ORDER BY id'
            else:
                sql = 'SELECT id,name,path_edgelist,path_graph_gt FROM %s WHERE (path_edgelist IS NOT NULL OR path_graph_gt IS NOT NULL) ORDER BY id' % args['db_tbname']
            
            cur = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cur.execute( sql, names )

            datasets = cur.fetchall()
            cur.close()

        else:
            datasets = args['from_file']        # argparse returns [[..], [..]]
            datasets = list( map( lambda ds: {        # to be compatible with existing build_graph function we transform the array to a dict
                'name': ds[0], 
                'path_edgelist': 'dumps/%s/data.edgelist.csv' % ds[0], 
                'path_graph_gt': 'dumps/%s/data.graph.gt.gz' % ds[0] }, datasets ) )
            
            names = ', '.join( map( lambda d: d['name'], datasets ) )
            log.debug( 'Configured datasets: %s', names )

        if args['build_graph']:

            # init feature list
            if len( args['features'] ) == 0:
                # eigenvector_centrality, global_clustering and local_clustering left out due to runtime
                args['features'] = ['degree', 'plots', 'diameter', 'fill', 'h_index', 'pagerank', 'parallel_edges', 'powerlaw', 'reciprocity']

            build_graph( datasets, args['threads'], args['threads_openmp'] )

        elif args['dump_graph']:
            # this is only respected when --dump-graph is specified without --build-graph (that's why the elif)
            # --dump-graph is respected in the build_graph function, when specified together with --build-graph.
            
            # TODO ZL respect --file-file
            datasets = cur.fetchall()
            cur.close()

            for ds in datasets:
                stats = {}
                g = load_graph_from_edgelist( ds, stats )

                if not g:
                    log.error( 'Could not instantiate graph for dataset %s', ds['name'] )
                    continue

                log.info( 'Dumping graph..' )
                graph_gt = '/'.join( [os.path.dirname( ds['path_edgelist'] ),'data.graph.gt.gz'] )
                g.save( graph_gt )
                stats['path_graph_gt'] = graph_gt

                # thats it here
                save_stats( ds, stats )
                continue

    # close communication with the database
    if args['from_db']:
        conn.close()

# -----------------
#
# notes
# - add error-column to table and set it
