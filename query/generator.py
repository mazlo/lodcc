from graph_tool.all import *
import argparse
from importlib import import_module
import logging as log
import os
import pystache
import re
import sys
import xxhash as xh

from util.bfv_from_file import job_find_vertices

def slice_url( e, prefixes={} ):
    """"""
    # '<http://purl.org/asit/terms/hasTown>' becomes
    # ( 'http://purl.org/asit/terms/', 'hasTown' )
    
    # because e is a dictionary entry
    h, url  = e
    # extracts the property from a url, e.g. "hasTown" from <http://purl.org/asit/terms/hasTown>
    m       = re.search( '<.*(/|#)(.*)>$', url )
    
    if not m:
        return e
    
    cut_idx = url.index( m.group(2) )

    prefix  = url[1:cut_idx]    # e.g. http://purl.org/asit/terms/
    m       = re.search( '.*/(.*)(/|#)$', prefix )  # extracts the last part of the url as prefix, e.g. "terms" from http://purl.org/asit/terms/
    hprefix = re.sub( '[^a-z]', '', m.group(1) )
    hprefix = re.sub( 'rdfsyntaxns', 'rdf', hprefix )
    hprefix = re.sub( 'rdfschema', 'rdfs', hprefix )
    prop    = url[cut_idx:-1]   # e.g. hasTown
    
    # a map of hashed urls to their url
    # e.g. { 'e123': 'http://../', 'abc1': 'http://..#' }
    prefixes[hprefix] = prefix
    
    return ( h , '%s:%s' % ( hprefix, prop ) )
    
def prefix_it( hmap ):
    """"""
    # input: hmap = { 'e0: '<http://...', 'e1': '<http://...', ... }
    
    # supposed to be a map of hashed urls to their url
    # e.g. { 'e123': 'http://../', 'abc1': 'http://..#' }
    hprefixes = {}
    
    # after: hmap = { 'e0': 'prefix:property1', 'e1': 'prefix:property2', ... }
    hmap = dict( map( lambda e: slice_url( e, hprefixes ), hmap.items() ) )
    
    # after: prefixes = [ { 'prefix': 'PREFIX prefix1: <http://...> .' }, { 'prefix': 'PREFIX prefix2: <..> .' }, ... ]
    prefixes = list( map( lambda e: { 'prefix': 'PREFIX %s: <%s>' % (e[0], e[1]) }, hprefixes.items() ) )
    
    # add to the dictionary
    hmap['prefixes'] = prefixes

    return hmap

def instantiate_query( D, QG, template, dataset, max_n=3 ):
    """instantiates the query given by the template"""
    
    log.debug( 'finding subgraph isomorphism' )
    I=subgraph_isomorphism( QG, D, max_n=max_n )

    queries = []
    
    if len(I) == 0:
        log.warn( 'No isomorphisms found' )
        return queries
    
    for i in range(len(I)):
        pmap = I[i]
        
        log.debug( 'creating edge hash-map' )
        
        # after: [ [ 0,src_vertex,trg_vertex ], [ 1,src_vertex,trg_vertex ], ... ]
        D_edges = list( map( lambda e: [ QG.edge_index[e], pmap.fa[int( e.source() )], pmap.fa[int( e.target() )] ], QG.edges() ) )
        log.debug( D_edges )
        
        log.debug( 'creating vertices hash-map' )
        
        # after: {'e0': 'ae98476863dc6ec5', 'e0_subj': 'b3101bcc997b3d96', 'e0_obj': '80c23150a161b2d1', ... }
        mmap = {}
        
        for e in D_edges:
            # e.g. { 'e0': 'ae98476863dc6ec5', 'e1': '00c4ee7beb8097f0', .. }
            mmap['e%s' % e[0]] = D.ep.c0[ (e[1],e[2]) ]
            # e.g. { 'e0_subj': 'b3101bcc997b3d96' }, the source of the edge e0
            mmap['e%s_subj' % e[0]] = D.vp.name[ e[1] ]
            # e.g. { 'e0_obj': '80c23150a161b2d1' }, the target of the edge e0
            mmap['e%s_obj' % e[0]] = D.vp.name[ e[2] ]
        
        log.debug( mmap )
        
        log.debug( 'resolving hashes to URIs from nt-files in folder %s' % dataset )
        hmap = job_find_vertices( dataset, list(mmap.values()) )

        # after: { 'e0: '<http://...', 'e1': '<http://...', ... }
        hmap = dict( map( lambda t: (t[0], hmap[t[1]]) if t[1] in hmap else t, mmap.items() ) )
        
        log.debug( hmap )
        log.debug( 'Resolving prefixes ..' )
        
        # after: { 'e0': 'prefix1:prop1', 'e1': 'prefix2:prop2', ... }
        hmap = prefix_it( hmap )

        log.debug( 'Rendering template %s' % template )
        # the real query
        query = pystache.render( template, hmap )
        queries.append( query )
    
    return queries

def generate_queries( D, queries, dataset, no=1 ):
    """"""

    log.debug( 'Entering generate_queries' )

    if type(queries) == int:
        queries = range( queries, queries+1 )
    
    log.info( 'Rendering queries ..' )
    for query_name in queries:

        query_template = '%s/%s.tpl' % (args['query_templates_folder'],query_name)
        query_graph = 'query_graph_%s' % query_name
        
        log.debug( query_name )
        log.debug( query_template )
        log.debug( query_graph )
        
        if not os.path.isfile( query_template ):
            log.error( 'no query template found for query %s', query_name )
            continue
        
        if not hasattr( _module, query_graph ):
            log.error( 'no query graph found for query %s', query_name )
            continue
        
        QG = getattr( _module, query_graph )()         # the query-graph, represented as Graph-object
        QT = open( query_template, 'r' ).read()     # the query-template, as mustache template

        if args['log_debug']:
            graph_draw( QG, output_size=(200,200) )
        
        log.debug( 'Rendering query %s' % query_name.upper() )
        instances = instantiate_query( D, QG, QT, dataset['folder'], no )
        
        if len( instances ) == 0:
            log.warn( 'Could not instantiate query' )
            continue
        
        for idx,q in enumerate( instances ):
            with open( '%s/queries_%s/%s%s.sparql' % ( args['output_folder'], dataset['name'], query_name, '' if len( instances ) == 1 else '_%s' % idx+1 ), 'w' ) as qf:
                qf.write( q )

def load_graph_from_edgelist( dataset ):
    """"""

    log.debug( 'Entering load_graph_from_edgelist' )
    edgelist, graph_gt = dataset['path_edgelist'], dataset['path_graph_gt']

    D=None

    # prefer graph_gt file
    if graph_gt and os.path.isfile( graph_gt ):
        log.info( 'Constructing DiGraph from gt.xz' )
        D=load_graph( graph_gt )
    
    elif edgelist and os.path.isfile( edgelist ):
        log.info( 'Constructing DiGraph from edgelist' )

        D=load_graph_from_csv( edgelist, directed=True, string_vals=True, skip_first=False, csv_options={'delimiter': ' ', 'quotechar': '"'} )
    
    else:
        log.error( 'edgelist or graph_gt file to read graph from does not exist' )
        return None

    return D

if __name__ == '__main__':

    # configure args parser
    parser = argparse.ArgumentParser( description = 'generator - instantiate common queries from a given dataset' )
    parser.add_argument( '--datasets', '-d', action = "append", required = True, help = '', nargs = '*')
    parser.add_argument( '--queries', '-q', action = "append", help = '', nargs = '*', type=str )
    
    parser.add_argument( '--query-graphs', '-qg', required = False, type=str, default = 'query.watdiv.query_graphs', help = 'The python module to import the graph graphs from. Example parameter value: "query.watdiv.query_graphs".' )
    parser.add_argument( '--query-templates-folder', '-qf', required = False, type=str, default = 'query/watdiv/templates', help = 'The folder where to find the query templates. Example parameter value: "query/watdiv/templates".' )
    
    parser.add_argument( '--output-folder', '-o', required = False, type = str, default = 'target' )
    # TODO ZL add param --instances-per-query
    # TODO ZL add param --instances-choose

    parser.add_argument( '--log-debug', action='store_true', help = '' )

    args = vars( parser.parse_args() )

    if args['log_debug']:
        level = log.DEBUG
    else:
        level = log.INFO

    # configure log
    log.basicConfig( level = level, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    z = vars( parser.parse_args() ).copy()
    z.update( args )
    args = z

    # import query graph methods
    _module = args['query_graphs']

    try:
        _module = import_module( _module )
    except:
        log.debug( 'Query graphs module: %s', _module )
        log.error( 'Could not find module with query graphs, which is required.' )
        sys.exit(0)

    # check query templates folder
    if not os.path.isdir( args['query_templates_folder'] ):
        log.debug( 'Query templates folder: %s', args['query_templates_folder'] )
        log.error( 'Could not find folder with query templates, which is required.' )
        sys.exit(0)

    # 
    datasets = args['datasets']        # argparse returns [[..], [..]]
    datasets = list( map( lambda ds: {        # to be compatible with existing build_graph function we transform the array to a dict
        'name': ds[0], 
        'folder': 'dumps/%s' % ds[0],
        'path_edgelist': 'dumps/%s/data.edgelist.csv' % ds[0], 
        'path_graph_gt': 'dumps/%s/data.graph.gt.gz' % ds[0] }, datasets ) )
            
    names = ', '.join( map( lambda d: d['name'], datasets ) )
    log.debug( 'Configured datasets: %s', names )

    queries = args['queries']
    if not queries or len( queries ) == 0:
        queries = range(1,21)   # we got 20 queries
    else:
        queries = queries[0]

    log.debug( 'Configured queries: %s', queries)

    if not os.path.isdir( args['output_folder'] ):
        os.mkdir( args['output_folder'] )   # e.g. target
        for dataset in datasets:
            target_folder = '%s/queries_%s' % (args['output_folder'], dataset['name'])
            if not os.path.isdir( target_folder ):
                os.mkdir( target_folder )   # e.g. target/queries_lexvo

    for dataset in datasets:
        D = load_graph_from_edgelist( dataset )

        if not D:
            log.error( 'Could not instantiate graph for dataset %s', dataset['name'] )
            continue

        generate_queries( D, queries, dataset )

    log.info( 'Done' )
