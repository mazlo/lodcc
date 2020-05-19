import argparse
import logging as log
import os
import re
import subprocess as proc
import threading
from urllib import parse as urlparse
import xxhash as xh

# lodcc module imports
from constants.preparation import *
from util.lxxhash import xxhash_nt
from util.merge_edgelists import merge_edgelists

def download_prepare( dataset, from_file ):
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
    
    if from_file:
        # if run --from-file, the format is given on cli not from db column
        log.debug( 'Using format passed as third parameter with --from-file' )
        
        if len( dataset ) != 4:
            log.error( 'Missing third format argument in --from-file. Please specify' )
            return [( None, APPLICATION_UNKNOWN )]

        if not dataset[3] in SHORT_FORMAT_MAP:
            log.error( 'Wrong format "%s". Please specify one of %s', dataset[3], ','.join( SHORT_FORMAT_MAP.keys()) )
            return [( None , APPLICATION_UNKNOWN )]
        
        urls.append( ( dataset[2], SHORT_FORMAT_MAP[dataset[3]] ) )
        return urls

    log.debug( 'Determining available formats..' )
    # this list of if-else's also respects db column priority

    # n-triples
    if len( dataset ) >= 3 and dataset[2]:
        log.debug( 'Found format APPLICATION_N_TRIPLES with url %s', dataset[2] )
        urls.append( ( dataset[2], APPLICATION_N_TRIPLES ) )

    # rdf+xml
    if len( dataset ) >= 4 and dataset[3]:
        log.debug( 'Found format APPLICATION_RDF_XML with url: %s', dataset[3] )
        urls.append( ( dataset[3], APPLICATION_RDF_XML ) )

    # turtle
    if len( dataset ) >= 5 and dataset[4]:
        log.debug( 'Found format TEXT_TURTLE with url: %s', dataset[4] )
        urls.append( ( dataset[4], TEXT_TURTLE ) )

    # notation3
    if len( dataset ) >= 6 and dataset[5]:
        log.debug( 'Found format TEXT_N3 with url: %s', dataset[5] )
        urls.append( ( dataset[5], TEXT_N3 ) )

    # nquads
    if len( dataset ) >= 7 and dataset[6]:
        log.debug( 'Found format APPLICATION_N_QUADS with url: %s', dataset[6] )
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
        filename = '%s_%s%s' % (dataset[1], dataset[0], MEDIATYPES[format_]['extension'])
        log.debug( 'Cannot determine filename from remaining url path: %s', url.path )
        log.debug( 'Using composed valid filename %s', filename )
        
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
            log.error( 'Could not continue due to unknown format. ignoring this one..' )
            continue

        filename = ensure_valid_filename_from_url( dataset, url, format_ )
        folder = '/'.join( ['dumps', dataset[1]] )
        path = '/'.join( [ folder, filename ] )

        # reuse dump if exists
        valid = ensure_valid_download_data( path )
        if not args['overwrite_dl'] and valid:
            log.debug( 'Overwrite dl? %s. Reusing local dump', args['overwrite_dl'] )
            return dict( { 'path': path, 'filename': filename, 'folder': folder, 'format': format_ } )

        # download anew otherwise
        # thread waits until this is finished
        log.info( 'Downloading dump (from %s) ...', url )
        os.popen( 'wget --quiet --output-document %s %s' % (path,url)  )

        valid = ensure_valid_download_data( path )
        if not valid:
            log.warn( 'Skipping format %s. Trying with other format if available.', format_ )
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

    overwrite_nt = 'true' if args['overwrite_nt'] else 'false'
    rm_original  = 'true' if args['rm_original'] else 'false'

    # transform into ntriples if necessary
    # TODO do not transform if file has ntriples format
    # TODO check content of file
    # TODO check if file ends with .nt
    log.info( 'Transforming to ntriples..' )
    log.debug( 'Overwrite nt? %s', overwrite_nt )
    log.debug( 'Remove original file? %s', rm_original )
    log.debug( 'Calling command %s', MEDIATYPES[format_]['cmd_to_ntriples'] % (path,overwrite_nt,rm_original) )
    
    proc.call( MEDIATYPES[format_]['cmd_to_ntriples'] % (path,overwrite_nt,rm_original), shell=True )

    # TODO check correct mediatype if not compressed

    # transform into hashed edgelist
    log.info( 'Preparing edgelist graph structure..' )
    log.debug( 'Calling function xxhash_nt( %s )', path )
    
    types = [ type_ for type_ in MEDIATYPES_COMPRESSED if re.search( '.%s$' % type_, path ) ]
    if len( types ) == 0:
        # file it not compressed
        xxhash_nt( path, log )
    else:
        # file is compressed, strip the type
        xxhash_nt( re.sub( '.%s' % types[0], '', path ), log )

# real job
def job_start_download_and_prepare( dataset, sem, from_file ):
    """job_start_download_and_prepare"""

    # let's go
    with sem:
        log.info( 'Let''s go' )
        
        # - download_prepare
        urls = download_prepare( dataset, from_file )

        # - download_data
        file = download_data( dataset, urls )

        # - build_graph_prepare
        build_graph_prepare( dataset, file )

        log.info( 'Done' ) 

def job_cleanup_intermediate( dataset, rm_edgelists, sem ):
    """"""

    # can I?
    with sem:
        merge_edgelists( dataset, rm_edgelists, log )
    
def prepare_graph( datasets, no_of_threads=1, from_file=False ):
    """prepare_graph"""

    if len( datasets ) == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )
    threads = []

    for dataset in datasets:
        
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start_download_and_prepare, name = '%s[%s]' % (dataset[1],dataset[0]), args = ( dataset, sem, from_file ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

    # after all processing, merge edgelists
    datasets = set( [ ds[1] for ds in datasets] )
    rm_edgelists = 'false' if args['keep_edgelists'] else 'true'
    threads = []

    for dataset in datasets:
        
        t = threading.Thread( target = job_cleanup_intermediate, name = '%s' % dataset, args = ( dataset, rm_edgelists, sem ) )
        t.start()

        threads.append(t)
    
    # wait for all threads to finish
    for t in threads:
        t.join()
    
# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )

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
    parser.add_argument( '--threads', '-pt', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

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

    log.info( 'graph.building.preparation: Welcome' )

    # option 2
    if args['from_db']:
        # respect --use-datasets argument
        if args['use_datasets']:
            names_query = '( ' + ' OR '.join( 'name = %s' for ds in args['use_datasets'] ) + ' )'
            names = tuple( args['use_datasets'] )
        else:
            names = 'all'

        log.debug( 'Configured datasets: '+ ', '.join( names ) )

        if 'names_query' in locals():
            sql = 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats_2017_08 WHERE '+ names_query +' AND (application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL) ORDER BY id'
        else:
            sql = 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats_2017_08 WHERE application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL ORDER BY id'

        cur = conn.cursor()
        cur.execute( sql, names )

        datasets = cur.fetchall()
        cur.close()

    else:
        log.info( 'Requested to prepare graph from file' )
        datasets = args['from_file']        # argparse returns [[..], [..],..]
        # add an artificial id from hash. array now becomes [[id, ..],[id,..],..]
        datasets = list( map( lambda d: [xh.xxh64( d[0] ).hexdigest()[0:4]] + d, datasets ) )
        names = ', '.join( map( lambda d: d[1], datasets ) )
        log.debug( 'Configured datasets: %s', names )

    prepare_graph( datasets, None if 'threads' not in args else args['threads'], args['from_file'] )
