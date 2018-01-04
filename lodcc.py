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
import psycopg2
import threading
import sys
import urlparse

from constants import *

mediatype_mappings = {}

def ensure_db_schema_complete( cur, attribute ):
    ```ensure_db_schema_complete```

    log.debug( 'Checking if column %s exists', attribute )
    cur.execute( "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_name = %s;", ('stats', attribute) )

    if cur.rowcount == 0:
        log.info( 'Creating missing attribute %s', attribute )
        cur.execute( "ALTER TABLE stats ADD COLUMN "+ attribute +" varchar;" )

    log.debug( 'Found %s-attribute', attribute )
    return attribute

def ensure_db_record_is_unique( cur, name, attribute, value ):
    ```ensure_db_record_is_unique```

    cur.execute( 'SELECT id FROM stats WHERE name = %s AND ('+ attribute +' IS NULL OR '+ attribute +' = %s)', (name, "") )

    if cur.rowcount != 0:
        # returns the id of the row to be updated
        return cur.fetchone()[0]
    else:
        # insert new row and return the id of the row to be updated
        log.info( 'Attribute %s not unique for "%s". Will create a new row.', attribute, name )
        cur.execute( 'INSERT INTO stats (id, name, '+ attribute +') VALUES (default, %s, %s) RETURNING id', (name, value) )

        return cur.fetchone()[0]

def ensure_format_in_dictionary( format_ ):
    ```ensure_format_in_dictionary```

    if format_ in mediatype_mappings:
        log.info( 'Format %s will be mapped to %s', format_, mediatype_mappings[format_] )
        return mediatype_mappings[format_]

    return format_

def ensure_format_is_valid( r ):
    ```ensure_format_is_valid```

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

def save_value( cur, dataset_id, dataset_name, attribute, value, check=True ):
    ```save_value```

    ensure_db_schema_complete( cur, attribute )

    if check and not value:
        # TODO create warning message
        log.warn( 'No value for attribute '+ attribute +'. Cannot save' )
        return
    elif check:
        # returns the id of the row to be updated
        dataset_id = ensure_db_record_is_unique( cur, dataset_name, attribute, value )
    
    log.debug( 'Saving value "%s" for attribute "%s" for "%s"', value, attribute, dataset_name )
    cur.execute( 'UPDATE stats SET '+ attribute +' = %s WHERE id = %s;', ( value, dataset_id ) )

def parse_datapackages( dataset_id, datahub_url, name, dry_run=False ):
    ```parse_datapackages```

    dp = None

    datapackage_filename = 'datapackage_'+ name +'.json'
    if not os.path.isfile( datapackage_filename ):
        log.info( 'cURLing datapackage.json for %s', name )
        os.popen( 'curl -s -L "'+ datahub_url +'/datapackage.json" -o '+ datapackage_filename )
        # TODO ensure the process succeeds
    else:
        log.info( 'Using local datapackage.json for %s', name )

    with open( 'datapackage_'+ name +'.json', 'r' ) as file:
        try:
            log.debug( 'Parsing datapackage.json' )
            dp = json.load( file )

            if 'name' in dp:
                name = dp['name']
                save_value( cur, dataset_id, name, 'name', name, False )
            else:
                log.warn( 'No name-property given. File will be saved in datapackage.json' )

            if not 'resources' in dp:
                log.error( '"resources" does not exist for %s', name )
                # TODO create error message and exit
                return None

            log.debug( 'Found resources-object. reading' )
            for r in dp['resources']:

                format_ = ensure_format_is_valid( r )

                if not format_:
                    continue

                save_value( cur, dataset_id, name, format_, r['url'], True )

            save_value( cur, dataset_id, name, 'keywords', dp['keywords'] if 'keywords' in dp else None, False )
            # save whole datapackage.json in column
            save_value( cur, dataset_id, name, 'datapackage_content', str( json.dumps( dp ) ), False )

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

    log.info( 'Download folder will be %s', 'dumps/'+ dataset[1] )
    os.popen( 'mkdir -p dumps/'+ dataset[1] )

    # id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads

    urls = list()

    # this list of if-else's also respects priority

    # n-triples
    if len( dataset ) >= 3 and dataset[2]:
        log.info( 'Using format APPLICATION_N_TRIPLES with url %s', dataset[2] )
        urls.append( ( dataset[2], APPLICATION_N_TRIPLES ) )

    # rdf+xml
    if len( dataset ) >= 4 and dataset[3]:
        log.info( 'Using format APPLICATION_RDF_XML with url: %s', dataset[3] )
        urls.append( ( dataset[3], APPLICATION_RDF_XML ) )

    # turtle
    if len( dataset ) >= 5 and dataset[4]:
        log.info( 'Using format TEXT_TURTLE with url: %s', dataset[4] )
        urls.append( ( dataset[4], TEXT_TURTLE ) )

    # notation3
    if len( dataset ) >= 6 and dataset[5]:
        log.info( 'Using format TEXT_N3 with url: %s', dataset[5] )
        urls.append( ( dataset[5], TEXT_N3 ) )

    # nquads
    if len( dataset ) >= 7 and dataset[6]:
        log.info( 'Using format APPLICATION_N_QUADS with url: %s', dataset[6] )
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

    log.info( 'Found valid filename %s', basename )
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

    if 'void' in os.path.basename( path ):
        # TODO save error in db
        log.warn( 'Download not valid: file contains probably void descriptions, not data (%s)', path )
        return False

    return True

def download_data( dataset, urls ):
    ```download_data```

    for url, format_ in urls:

        if format_ == APPLICATION_UNKNOWN:
            log.error( 'Could not continue due to unknown format. %s', dataset[1] )
            continue

        filename = ensure_valid_filename_from_url( dataset, url, format_ )
        folder = '/'.join( ['dumps', dataset[1]] )
        path = '/'.join( [ folder, filename ] )

        # reuse dump if exists
        valid = ensure_valid_download_data( path )
        if not args['no_cache'] and valid:
            log.info( 'Reusing dump for %s', dataset[1] )
            return folder, filename, format_

        # download anew otherwise
        # thread waits until this is finished
        log.info( 'Downloading dump for %s ...', dataset[1] )
        os.popen( 'curl -s -L "'+ url +'" -o '+ path  )

        valid = ensure_valid_download_data( path )
        if not valid:
            log.info( 'Skipping format %s', format_ )
            continue
        else:
            return folder, filename, format_

    return None, None, None

def get_file_mediatype( filename ):
    """get_file_mediatype

    returns ('tgz', True) for 'foo.bar.tgz' (filename ends with a compressed mediatype). 
    returns ('bar.nt', False) for 'foo.bar.nt' (filename does not end with compressed mediatype).
    returns ('foo', False) for 'foo' (filename has no mediatype).
    """

    idx = filename.find( '.' )
    if idx <= 0:
        log.error( 'No file extension found for: %s', filename )
        return ( filename, False )

    mediatype = filename[idx:]

    types = [ type_ for type_ in MEDIATYPES_COMPRESSED if mediatype.endswith( '.'+ type_ ) ]
    if len( types ) == 0:
        return ( filename[idx+1:], False )

    return ( types[0], True )

def build_graph_prepare( dataset, details ):
    ```build_graph_prepare```

    if not details:
        log.error( 'Cannot continue due to error in downloading data. returning.' )
        return

    folder, filename, format_ = details

    if not filename:
        log.error( 'Cannot prepare graph for %s, aborting', dataset[1] )
        return

    # decompress if necessary
    filespec = get_file_mediatype( filename )
    if filespec[1]:
        log.info( 'Need to decompress %s', filename )

        os.popen( MEDIATYPES[format_]['cmd_to_one-liner'] % ( folder, filename, '.'+ filespec[0] ) )

        filename = re.sub( '.'+ filespec[0], '', filename )
    
    # TODO check correct mediatype if not compressed

    no_cache = 'true' if args['no_cache'] else ''

    path = '/'.join( [ folder, filename ] )

    # transform into ntriples if necessary
    if not format_ == APPLICATION_N_TRIPLES:
        log.info( 'Need to transform to ntriples.. this may take a while' )
        os.popen( MEDIATYPES[format_]['cmd_to_ntriples'] % (path,no_cache) )

    # transform into graph csv
    log.info( 'Preparing required graph structure.. this may take a while' )
    os.popen( MEDIATYPES[format_]['cmd_to_csv'] % (path,no_cache) )

# real job
def job_start( dataset, sem ):
    ```job_start```

    # let's go
    with sem:
        # - download_prepare
        urls = download_prepare( dataset )

        # - download_data
        url_details = download_data( dataset, urls )

        # - build_graph_prepare
        build_graph_prepare( dataset, url_details )

        # - build_graph_analyse

        # - job_cleanup

        log.info( 'Done' ) 

def parse_resource_urls( cur, no_of_threads=1 ):
    ```parse_resource_urls```

    datasets = cur.fetchall()

    if cur.rowcount == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )

    threads = []

    for dataset in datasets:
        
        log.info( 'Starting job for %s', dataset )
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start, name = 'Job: '+ dataset[1], args = ( dataset, sem ) )
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
    parser.add_argument( '--dry-run', '-d', action = "store_true", help = '' )
    parser.add_argument( '--use-datasets', '-du', nargs='*', help = '' )
    parser.add_argument( '--no-cache', '-dn', action = "store_true", help = 'Will NOT use data dumps which were already dowloaded, but download them again' )
    parser.add_argument( '--log-level-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-level-info', '-li', action = "store_true", help = '' )
    parser.add_argument( '--log-file', '-lf', required = False, type = str, default = 'lodcc.log', help = '' )
    parser.add_argument( '--threads', '-pt', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

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
        log.basicConfig( filename = args['log_file'], level = log.DEBUG, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )
    else:
        log.basicConfig( filename = args['log_file'], level = log.INFO, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )
    
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
        if args['dry_run']:
            log.info( 'Running in dry-run mode' )

            if args['use_datasets']:
                names_query = '( ' + ' OR '.join( 'name = %s' for ds in args['use_datasets'] ) + ' )'
                names = tuple( args['use_datasets'] )
            else:
                names_query = 'name = %s'
                names = tuple( ['museums-in-italy'] )

            log.debug( 'Configured datasets: '+ ', '.join( names ) )
            
            sql = 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats WHERE '+ names_query +' AND (application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL) ORDER BY id'

            cur.execute( sql, names )
        else:
            cur.execute( 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats WHERE application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL ORDER BY id' )

        parse_resource_urls( cur, None if 'threads' not in args else args['threads'] )


    # close communication with the database
    cur.close()
    conn.close()

# -----------------
#
# notes
# - add error-column to table and set it
