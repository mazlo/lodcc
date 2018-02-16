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
import json
import logging as log
import threading
import sys
import urlparse

from constants import *
try:
    import psycopg2
except:
    log.warning( 'psycogp2 could not be found' )
try:
    from lxxhash import xxhash_nt
except:
    log.warning( 'xxhash module could not be found' )

mediatype_mappings = {}

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

def ensure_format_in_dictionary( format_ ):
    """ensure_format_in_dictionary"""

    if format_ in mediatype_mappings:
        log.info( 'Format %s will be mapped to %s', format_, mediatype_mappings[format_] )
        return mediatype_mappings[format_]

    return format_

def ensure_format_is_valid( r ):
    """ensure_format_is_valid"""

    if not 'format' in r:
        log.error( 'resources-object is missing format-property. Cannot save this value' )
        # TODO create error message and exit
        return None

    format_ = r['format'].strip().lower()
    format_ = re.sub( r'[^a-zA-Z0-9]', '_', format_ )  # replace special character in format-attribute with _
    format_ = re.sub( r'^_+', '', format_ )  # replace leading _
    format_ = re.sub( r'_+$', '', format_ )  # replace trailing _
    format_ = re.sub( r'__*', '_', format_ )  # replace double __

    if not format_:
        log.error( 'Format is not valid after cleanup, original: %s. Will continue with next resource', r['format'] )
        return None

    format_ = ensure_format_in_dictionary( format_ )

    log.info( 'Found valid format "%s"', format_ )

    return format_

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

def parse_datapackages( dataset_id, datahub_url, dataset_name, dry_run=False ):
    """parse_datapackages"""

    dp = None

    datapackage_filename = 'datapackage_'+ dataset_name +'.json'
    if not os.path.isfile( datapackage_filename ):
        log.info( 'cURLing datapackage.json for %s', dataset_name )
        os.popen( 'curl -s -L "'+ datahub_url +'/datapackage.json" -o '+ datapackage_filename )
        # TODO ensure the process succeeds
    else:
        log.info( 'Using local datapackage.json for %s', dataset_name )

    with open( 'datapackage_'+ dataset_name +'.json', 'r' ) as file:
        try:
            log.debug( 'Parsing datapackage.json' )
            dp = json.load( file )

            if 'name' in dp:
                dataset_name = dp['name']
                save_value( cur, dataset_id, dataset_name, 'stats', 'name', dataset_name, False )
            else:
                log.warn( 'No name-property given. File will be saved in datapackage.json' )

            if not 'resources' in dp:
                log.error( '"resources" does not exist for %s', dataset_name )
                # TODO create error message and exit
                return None

            log.debug( 'Found resources-object. reading' )
            for r in dp['resources']:

                format_ = ensure_format_is_valid( r )

                if not format_:
                    continue

                save_value( cur, dataset_id, dataset_name, 'stats', format_, r['url'], True )

            save_value( cur, dataset_id, dataset_name, 'stats', 'keywords', dp['keywords'] if 'keywords' in dp else None, False )
            # save whole datapackage.json in column
            save_value( cur, dataset_id, dataset_name, 'stats', 'datapackage_content', str( json.dumps( dp ) ), False )

        except:
            # TODO create error message and exit
            raise
            return None

    return 

# -----------------

def download_prepare( dataset ):
    """download_prepare

    returns a tuple of url and application media type, if it can be discovered from the given dataset. For instance,
    returns ( 'http://example.org/foo.nt', APPLICATION_N_TRIPLES ) if { _, _, http://example.org/foo.nt, ... } was passed."""

    if not dataset:
        log.error( 'dataset is None' )
        return [( None, APPLICATION_UNKNOWN )]

    if not dataset[1]:
        log.error( 'dataset name is None' )
        return [( None, APPLICATION_UNKNOWN )]

    log.debug( 'Download folder will be %s', 'dumps/'+ dataset[1] )
    os.popen( 'mkdir -p dumps/'+ dataset[1] )

    # id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads

    urls = list()

    # this list of if-else's also respects priority

    # n-triples
    if len( dataset ) >= 3 and dataset[2]:
        log.debug( 'Using format APPLICATION_N_TRIPLES with url %s', dataset[2] )
        urls.append( ( dataset[2], APPLICATION_N_TRIPLES ) )

    # rdf+xml
    if len( dataset ) >= 4 and dataset[3]:
        log.debug( 'Using format APPLICATION_RDF_XML with url: %s', dataset[3] )
        urls.append( ( dataset[3], APPLICATION_RDF_XML ) )

    # turtle
    if len( dataset ) >= 5 and dataset[4]:
        log.debug( 'Using format TEXT_TURTLE with url: %s', dataset[4] )
        urls.append( ( dataset[4], TEXT_TURTLE ) )

    # notation3
    if len( dataset ) >= 6 and dataset[5]:
        log.debug( 'Using format TEXT_N3 with url: %s', dataset[5] )
        urls.append( ( dataset[5], TEXT_N3 ) )

    # nquads
    if len( dataset ) >= 7 and dataset[6]:
        log.debug( 'Using format APPLICATION_N_QUADS with url: %s', dataset[6] )
        urls.append( ( dataset[6], APPLICATION_N_QUADS ) )

    # more to follow?

    if len( urls ) == 0:
        log.warn( 'Could not determine format. returning APPLICATION_UNKNOWN instead' )
        return [( None, APPLICATION_UNKNOWN )]
    
    return urls
    
def ensure_valid_filename_from_url( dataset, url, format_ ):
    """ensure_valid_filename_from_url

    returns 'foo-bar.tar.gz' for url 'http://some-domain.com/foo-bar.tar.gz (filename is obtained from url), if invoked with ( [_], _, _ )'
    returns 'foo-dump.rdf' for url 'http://some-domain.com/strange-url (filename is NOT obtained from url), if invoked with ( [_, 'foo-dump.rdf', _], _, APPLICATION_RDF_XML )'
    """

    if not url:
        log.warn( 'No url given for %s. Cannot determine filename.', dataset[1] )
        return None

    log.debug( 'Parsing filename from %s', url )
    # transforms e.g. "https://drive.google.com/file/d/0B8VUbXki5Q0ibEIzbkUxSnQ5Ulk/dump.tar.gz?usp=sharing" 
    # into "dump.tar.gz"
    url = urlparse.urlparse( url )
    basename = os.path.basename( url.path )

    if not '.' in basename:
        filename = dataset[1] + MEDIATYPES[format_]['extension']
        log.warn( 'Cannot determine filename from remaining url path: %s', url.path )
        log.info( 'Using composed valid filename %s', filename )
        
        return filename

    log.debug( 'Found valid filename %s', basename )
    return basename

def ensure_valid_download_data( path ):
    """ensure_valid_download_data"""

    if not os.path.isfile( path ):
        # TODO save error in db
        log.warn( 'Download not valid: file does not exist (%s)', path )
        return False

    if os.path.getsize( path ) < 1000:
        # TODO save error in db
        log.warn( 'Download not valid: file is < 1000 byte (%s)', path )
        return False

    if 'void' in os.path.basename( path ) or 'metadata' in os.path.basename( path ):
        # TODO save error in db
        log.warn( 'Download not valid: file contains probably void or metadata descriptions, not data (%s)', path )
        return False

    return True

def download_data( dataset, urls ):
    """download_data"""

    for url, format_ in urls:

        if format_ == APPLICATION_UNKNOWN:
            log.error( 'Could not continue due to unknown format. %s', dataset[1] )
            continue

        filename = ensure_valid_filename_from_url( dataset, url, format_ )
        folder = '/'.join( ['dumps', dataset[1]] )
        path = '/'.join( [ folder, filename ] )

        # reuse dump if exists
        valid = ensure_valid_download_data( path )
        if not args['overwrite_dl'] and valid:
            log.debug( 'Reusing dump for %s', dataset[1] )
            return dict( { 'path': path, 'filename': filename, 'folder': folder, 'format': format_ } )

        # download anew otherwise
        # thread waits until this is finished
        log.info( 'Downloading dump for %s ...', dataset[1] )
        os.popen( 'wget --quiet --output-document %s %s' % (path,url)  )

        valid = ensure_valid_download_data( path )
        if not valid:
            log.info( 'Skipping format %s', format_ )
            continue
        else:
            return dict( { 'path': path, 'filename': filename, 'folder': folder, 'format': format_ } )

    return dict()

def build_graph_prepare( dataset, file ):
    """build_graph_prepare"""

    if not file:
        log.error( 'Cannot continue due to error in downloading data. returning.' )
        return

    if not 'filename' in file:
        log.error( 'Cannot prepare graph for %s, aborting', dataset[1] )
        return

    format_ = file['format']
    path = file['path']

    overwrite = 'true' if args['overwrite_nt'] else 'false'
    rm_original  = 'true' if args['rm_original'] else 'false'
    rm_edgelists = 'false' if args['keep_edgelists'] else 'true'

    # transform into ntriples if necessary
    if not format_ == APPLICATION_N_TRIPLES:
        # TODO do not transform if file has ntriples format
        # TODO check content of file
        # TODO check if file ends with .nt
        log.info( 'Need to transform to ntriples.. this may take a while' )
        log.debug( 'Calling command %s', MEDIATYPES[format_]['cmd_to_ntriples'] % (path,overwrite,rm_original) )
        os.popen( MEDIATYPES[format_]['cmd_to_ntriples'] % (path,overwrite,rm_original) )

    # TODO check correct mediatype if not compressed

    # transform into hashed edgelist
    log.info( 'Preparing required graph structure.. this may take a while' )
    log.debug( 'Calling function xxhash_nt( %s )', path )
    
    types = [ type_ for type_ in MEDIATYPES_COMPRESSED if re.search( '.%s$' % type_, path ) ]
    if len( types ) == 0:
        # file it not compressed
        xxhash_nt( path, log )
    else:
        # file is compressed, strip the type
        xxhash_nt( re.sub( '.%s' % types[0], '', path ), log )

    log.info( 'Merging edgelists, if necessary..' )
    log.debug( 'Calling command %s', MEDIATYPES[format_]['cmd_merge_edgelists'] % (os.path.dirname(path),rm_edgelists) )
    os.popen( MEDIATYPES[format_]['cmd_merge_edgelists'] % (os.path.dirname(path),rm_edgelists) )

def job_cleanup_intermediate( dataset, file ):
    """"""

    # TODO remove 1. decompressed and transformed 2. .nt file

try:
    from graph_tool.all import *
except:
    log.warning( 'graph_tool module could not be imported' )
import numpy as n
import collections
try:
    import matplotlib.pyplot as plt
except:
    log.warning( 'matplotlib.pyplot module could not be imported' )

lock = threading.Lock()

def fs_digraph_using_basic_properties( D, stats ):
    """"""

    eprop = label_parallel_edges( D, mark_only=True )
    PE = GraphView( D, efilt=eprop )
    num_edges_PE = PE.num_edges()

    # feature: order
    num_vertices = D.num_vertices()
    log.info( 'done order' )

    # feature: size
    num_edges = D.num_edges()
    log.info( 'done size' )

    stats['n']=num_vertices
    stats['m']=num_edges
    stats['m_unique']=num_edges - num_edges_PE

    # feature: avg_degree
    stats['avg_degree']=float( 2*num_edges ) / num_vertices
    log.info( 'done avg_degree' )
    
    # feature: fill_overall
    stats['fill_overall']=float( num_edges ) / ( num_vertices*num_vertices )
    log.info( 'done fill_overall' )

    # feature: parallel_edges
    stats['parallel_edges']=num_edges_PE
    log.info( 'done parallel_edges' )

    # feature: fill
    stats['fill']=float( stats['m_unique'] ) / ( num_vertices*num_vertices )
    log.info( 'done fill' )

def fs_digraph_using_degree( D, stats ):
    """"""

    # compute once
    degree_list = D.degree_property_map( 'total' ).get_array()
    degree_list = degree_list.tolist()

    # feature: max_degree
    stats['max_degree']=n.max( degree_list )
    log.info( 'done max_degree' )

    # feature: degree_centrality
    s = 1.0 / ( D.num_vertices()-1.0 )
    stats['avg_degree_centrality']=n.sum( [ d*s for d in degree_list ] )
    log.info( 'done avg_degree_centrality' )

    # info: vertex with largest degree centrality
    degree_list_idx=zip( degree_list, D.vertex_index )
    largest_degree_vertex=reduce( (lambda new_tpl, last_tpl: new_tpl if new_tpl[0] >= last_tpl[0] else last_tpl), degree_list_idx )
    stats['max_degree_vertex']=D.vertex_properties['name'][largest_degree_vertex[1]]
    log.info( 'done max_degree_vertex' )

    # feature: h_index_u
    degree_list.sort(reverse=True)
    
    h = 0
    for x in degree_list:
        if x >= h + 1:
            h += 1
        else:
            break

    stats['h_index_u']=h
    log.info( 'done h_index_u' )

    # feature: p_law_exponent
    min_degree = n.min( degree_list )
    sum_of_logs = 1 / n.sum( [ n.log( (float(d)/min_degree) ) for d in degree_list ] )
    stats['p_law_exponent'] = 1 + ( len(degree_list) * sum_of_logs )
    stats['p_law_exponent_dmin'] = min_degree
    log.info( 'done p_law_exponent' )

    # plot degree distribution
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
    plt.savefig( stats['files_path'] +'/'+ 'distribution_degree.pdf' )
    log.info( 'done plotting degree distribution' )

    lock.release()

def fs_digraph_using_indegree( D, stats ):
    """"""

    # compute once
    degree_list = D.get_in_degrees( D.get_vertices() )
    degree_list = degree_list.tolist()

    # feature: max_in_degree
    stats['max_in_degree']=n.max( degree_list )
    log.info( 'done max_in_degree' )

    # feature: avg_in_degree_centrality
    s = 1.0 / ( D.num_vertices()-1.0 )
    stats['avg_in_degree_centrality']=n.sum( [ d*s for d in degree_list ] )
    log.info( 'done avg_in_degree_centrality' )

    # feature: h_index_d
    degree_list.sort(reverse=True)
    
    h = 0
    for x in degree_list:
        if x >= h + 1:
            h += 1
        else:
            break
    
    stats['h_index_d']=h
    log.info( 'done h_index_d' )
    
    # plot degree distribution
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
    plt.savefig( stats['files_path'] +'/'+ 'distribution_in-degree.pdf' )
    log.info( 'done plotting in-degree distribution' )

    lock.release()

def fs_digraph_using_outdegree( D, stats ):
    """"""

    # compute once
    degree_list = D.get_out_degrees( D.get_vertices() )
    degree_list = degree_list.tolist()

    # feature: max_out_degree
    stats['max_out_degree']=n.max( degree_list )
    log.info( 'done max_out_degree' )

    # feature: avg_out_degree_centrality
    s = 1.0 / ( D.num_vertices()-1.0 )
    stats['avg_out_degree_centrality']=n.sum( [ d*s for d in degree_list ] )
    log.info( 'done avg_out_degree_centrality' )

def f_reciprocity( D, stats ):
    """"""

    stats['reciprocity']=edge_reciprocity(D)
    log.info( 'done reciprocity' )

def f_eigenvector_centrality( D, stats ):
    """"""

    if not args['do_heavy_analysis']:
        log.info( 'Skipping eigenvector_centrality' )
        return

    eigenvector_list = eigenvector(D)[1].get_array().tolist()
        
    # info: vertex with largest eigenvector value
    ev_list_idx=zip( eigenvector_list, D.vertex_index )
    largest_ev_vertex=reduce( (lambda new_tpl, last_tpl: new_tpl if new_tpl[0] >= last_tpl[0] else last_tpl), ev_list_idx )
    stats['max_eigenvector_vertex']=D.vertex_properties['name'][largest_ev_vertex[1]]
    log.info( 'done max_eigenvector_vertex' )

    eigenvector_list.sort( reverse=True )

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
    plt.savefig( stats['files_path'] +'/'+ 'distribution_eigenvector-centrality.pdf' )
    log.info( 'done plotting eigenvector_centrality' )

    lock.release()
        
def f_pagerank( D, stats ):
    """"""

    pagerank_list = pagerank(D).get_array().tolist()

    # info: vertex with largest pagerank value
    pr_list_idx=zip( pagerank_list, D.vertex_index )
    largest_pr_vertex=reduce( (lambda new_tpl, last_tpl: new_tpl if new_tpl[0] >= last_tpl[0] else last_tpl), pr_list_idx )
    stats['max_pagerank_vertex']=D.vertex_properties['name'][largest_pr_vertex[1]]
    log.info( 'done max_pagerank_vertex' )
    
    pagerank_list.sort( reverse=True )

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
    plt.savefig( stats['files_path'] +'/'+ 'distribution_pagerank.pdf' )
    log.info( 'done plotting pagerank distribution' )

    lock.release()

def save_stats( dataset, stats ):
    """"""

    # e.g. avg_degree=%(avg_degree)s, max_degree=%(max_degree)s, ..
    cols = ', '.join( map( lambda d: d +'=%('+ d +')s', stats ) )

    sql='UPDATE stats_graph SET '+ cols +' WHERE id=%(id)s'
    stats['id']=dataset[0]

    cur = conn.cursor()
    cur.execute( sql, stats )
    conn.commit()
    cur.close()

    log.debug( 'done saving results' )

def fs_digraph_start_job( dataset, D, stats ):
    """"""

    features = [ 
        # fs = feature set
        fs_digraph_using_basic_properties,
        fs_digraph_using_degree, fs_digraph_using_indegree, fs_digraph_using_outdegree,
        f_reciprocity,
        f_pagerank, 
        f_eigenvector_centrality,
    ]

    for ftr in features:
        ftr( D, stats )

    save_stats( dataset, stats )

def f_avg_shortest_path( U, stats, sem ):
    # can I?
    with sem:
        stats['avg_shortest_path']=nx.average_shortest_path_length(U)
        log.info( 'done avg_shortest_path' )

def f_global_clustering( U, stats ):
    """"""

    if not args['do_heavy_analysis']:
        log.info( 'Skipping global_clustering' )
        return

    stats['global_clustering']=global_clustering(U)[0]
    log.info( 'done global_clustering' )

def f_avg_clustering( U, stats ):
    """"""
    
    if not args['do_heavy_analysis']:
        log.info( 'Skipping avg_clustering' )
        return

    stats['avg_clustering']=n.mean( local_clustering(U, undirected=True).get_array().tolist() )
    log.info( 'done avg_clustering' )

def f_pseudo_diameter( U, stats ):
    """"""

    dist, ends = pseudo_diameter(U)
    stats['pseudo_diameter']=dist
    stats['pseudo_diameter_src_vertex']=U.vertex_properties['name'][ends[0]]
    stats['pseudo_diameter_trg_vertex']=U.vertex_properties['name'][ends[1]]
    log.info( 'done pseudo_diameter' )

def fs_ugraph_start_job( dataset, U, stats ):
    """"""

    features = [ 
        # fs = feature set
        f_global_clustering, f_avg_clustering, 
        # f_avg_shortest_path, 
        f_pseudo_diameter,
    ]

    for ftr in features:
        ftr( U, stats )
    
    save_stats( dataset, stats )

def graph_analyze( dataset, edgelists_path, stats ):
    """"""
    
    if not os.path.isdir( edgelists_path ):
        log.error( '%s to read edges from does not exist', edgelists_path )
        return

    # find edgelist file
    edgelist = None
    for filename in os.listdir( edgelists_path ):
        edgelist_file = '/'.join( [edgelists_path,filename] )
    
        if not re.search( 'edgelist.csv$', filename ):
            log.debug( 'Skipping %s', filename )
            continue

        edgelist = edgelist_file
        break

    if not os.path.isfile( edgelist ):
        log.error( 'edgelist.csv to read edges from does not exist' )
        return

    log.info( 'Constructing DiGraph from edgelist' )
    D=load_graph_from_csv( edgelist, directed=True, string_vals=True, hashed=True, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
    
    log.info( 'Computing feature set DiGraph' )
    fs_digraph_start_job( dataset, D, stats )
    
    log.info( 'Computing feature set UGraph' )
    fs_ugraph_start_job( dataset, D, stats )
    
    # slow
    #stats['k_core(U)']=nx.k_core(U)
    #stats['radius(U)']=nx.radius(U)
    
    # plot distributions

    return stats

def build_graph_analyse( dataset, threads_openmp=7 ):
    """"""

    # e.g. dataset[2] = 'dumps/dbpedia-en'
    if not dataset[2]:
        log.error( 'No path given for dataset %s', dataset[1] )
        return 

    # before starting off: limit the number of threads a graph_tool job may acquire
    graph_tool.openmp_set_num_threads( threads_openmp )

    stats = { 'files_path': dataset[2] }
    graph_analyze( dataset, dataset[2], stats )

    if args['print_stats']:
        print stats

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

# real job
def job_start_download_and_prepare( dataset, sem ):
    """job_start_download_and_prepare"""

    # let's go
    with sem:
        log.info( 'Let''s go' )
        
        # - download_prepare
        urls = download_prepare( dataset )

        # - download_data
        file = download_data( dataset, urls )

        # - build_graph_prepare
        build_graph_prepare( dataset, file )

        # - job_cleanup_intermediate
        job_cleanup_intermediate( dataset, file )

        log.info( 'Done' ) 

def parse_resource_urls( cur, no_of_threads=1 ):
    """parse_resource_urls"""

    datasets = cur.fetchall()

    if cur.rowcount == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )

    threads = []

    for dataset in datasets:
        
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start_download_and_prepare, name = 'Job: '+ dataset[1], args = ( dataset, sem ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

def build_graph( cur, no_of_threads=1, threads_openmp=7 ):
    """"""

    datasets = cur.fetchall()

    if cur.rowcount == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )

    threads = []

    for dataset in datasets:
        
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start_build_graph, name = 'Job: '+ dataset[1], args = ( dataset, sem, threads_openmp ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )
    parser.add_argument( '--parse-datapackages', '-pd', action = "store_true", help = '' )
    parser.add_argument( '--parse-resource-urls', '-pu', action = "store_true", help = '' )
    parser.add_argument( '--build-graph', '-pa', action = "store_true", help = '' )
    parser.add_argument( '--dry-run', '-d', action = "store_true", help = '' )

    parser.add_argument( '--use-datasets', '-du', nargs='*', help = '' )
    parser.add_argument( '--overwrite-dl', '-ddl', action = "store_true", help = 'If this argument is present, the program WILL NOT use data dumps which were already dowloaded, but download them again' )
    parser.add_argument( '--overwrite-nt', '-dnt', action = "store_true", help = 'If this argument is present, the program WILL NOT use ntriple files which were already transformed, but transform them again' )
    parser.add_argument( '--rm-original', '-dro', action = "store_true", help = 'If this argument is present, the program WILL REMOVE the original downloaded data dump file' )
    parser.add_argument( '--keep-edgelists', '-dke', action = "store_true", help = 'If this argument is present, the program WILL KEEP single edgelists which were generated. A data.edgelist.csv file will be generated nevertheless.' )
    
    parser.add_argument( '--log-level-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-level-info', '-li', action = "store_true", help = '' )
    parser.add_argument( '--log-stdout', '-lf', action = "store_true", help = '' )
    parser.add_argument( '--print-stats', '-lp', action= "store_true", help = '' )
    parser.add_argument( '--processes', '-pt', required = False, type = int, default = 1, help = 'Specify how many processes will be used for downloading and parsing' )

    # RE feature computation
    parser.add_argument( '--threads-openmp', '-ot', required = False, type = int, default = 7, help = 'Specify how many threads will be used for the graph analysis' )
    parser.add_argument( '--do-heavy-analysis', '-ah', action = "store_true", help = '' )

    # read all properties in file into args-dict
    if os.path.isfile( 'db.properties' ):
        with open( 'db.properties', 'rt' ) as f:
            args = dict( ( key.replace( '.', '-' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) )
    else:
        log.error( 'Please verify your settings in db.properties (file exists?)' )
        sys.exit()

    z = vars( parser.parse_args() ).copy()
    z.update( args )
    args = z
    
    if args['log_level_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    if args['log_stdout']:
        log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )
    else:
        log.basicConfig( filename = 'lodcc.log', filemode='w', level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )
    
    # read all format mappings
    if os.path.isfile( 'formats.properties' ):
        with open( 'formats.properties', 'rt' ) as f:
            # reads all lines and splits it so that we got a list of lists
            parts = list( re.split( "[=, ]+", option ) for option in ( line.strip() for line in f ) if option and not option.startswith( '#' ))
            # creates a hashmap from each multimappings
            mediatype_mappings = dict( ( format, mappings[0] ) for mappings in parts for format in mappings[1:] )

    # connect to an existing database
    conn = psycopg2.connect( host=args['db-host'], dbname=args['db-dbname'], user=args['db-user'], password=args['db-password'] )
    cur = conn.cursor()

    try:
        cur.execute( "SELECT 1;" )
        result = cur.fetchall()

        log.debug( 'Database ready to query execution' )
    except:
        log.error( 'Database not ready for query execution. %s', sys.exc_info()[0] )
        raise 

    # option 1
    if args['parse_datapackages']:
        if args['dry_run']:
            log.info( 'Running in dry-run mode' )
            log.info( 'Using example dataset "Museums in Italy"' )
    
            cur.execute( 'SELECT id, url, name FROM stats WHERE url = %s LIMIT 1', ('https://old.datahub.io/dataset/museums-in-italy') )
            
            if cur.rowcount == 0:
                log.error( 'Example dataset not found. Is the database filled?' )
                sys.exit()

            ds = cur.fetchall()[0]

            log.info( 'Preparing %s ', ds[2] )
            parse_datapackages( ds[0], ds[1], ds[2], True )

            conn.commit()
        else:
            cur.execute( 'SELECT id, url, name FROM stats' )
            datasets_to_fetch = cur.fetchall()
            
            for ds in datasets_to_fetch:
                log.info( 'Preparing %s ', ds[2] )
                parse_datapackages( ds[0], ds[1], ds[2] )
                conn.commit()

    # option 2
    if args['parse_resource_urls']:

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
            sql = 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats WHERE '+ names_query +' AND (application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL) ORDER BY id'
        else:
            sql = 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats WHERE application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL ORDER BY id'

        cur.execute( sql, names )

        parse_resource_urls( cur, None if 'processes' not in args else args['processes'] )

    # option 3
    if args['build_graph']:

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
            sql = 'SELECT id,name,files_path,filename FROM stats_graph WHERE '+ names_query +' AND filename IS NOT NULL ORDER BY id'
        else:
            sql = 'SELECT id,name,files_path,filename FROM stats_graph WHERE filename IS NOT NULL ORDER BY id'
        
        cur.execute( sql, names )

        build_graph( cur, args['processes'], args['threads_openmp'] )

    # close communication with the database
    cur.close()
    conn.close()

# -----------------
#
# notes
# - add error-column to table and set it
