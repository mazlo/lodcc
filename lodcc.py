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

APPLICATION_N_TRIPLES = 'application_n_triples'
APPLICATION_RDF_XML = 'application_rdf_xml'
APPLICATION_UNKNOWN = 'unknown'

mediatype_mappings = {}
mediatype_to_command = { 
    APPLICATION_RDF_XML: { 
        'cmd_to_ntriples': './to_ntriples.sh %s rdfxml', 
        'cmd_to_csv': './to_csv.sh %s', 
        'cmd_to_one-liner': '/to_one-liner.sh %s %s %s', # e.g. /to_one-liner.sh dumps/foo-dataset bar.nt.tgz .tgz
        'extension': '.rdf' 
    } 
}
mediatypes_compressed = [ 'tar.gz', 'tar.xz', 'tgz', 'gz', 'zip', 'bz2', 'tar' ]    # do not add 'xy.z' types at the end, they have privilege

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
    ```download_prepare```

    if not dataset:
        log.error( 'dataset is None' )
        return ( None, APPLICATION_UNKNOWN )

    if not dataset[1]:
        log.error( 'dataset name is None' )
        return ( None, APPLICATION_UNKNOWN )

    log.info( 'Download folder will be %s', 'dumps/'+ dataset[1] )
    os.popen( 'mkdir -p dumps/'+ dataset[1] )

    # id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads

    # n-triples
    if dataset[2]:
        log.info( 'Using format APPLICATION_N_TRIPLES with url %s', dataset[2] )
        return ( dataset[2], APPLICATION_N_TRIPLES )

    # rdf+xml
    elif dataset[3]:
        log.info( 'Using format APPLICATION_RDF_XML with url: %s', dataset[3] )
        return ( dataset[3], APPLICATION_RDF_XML )

    # more to follow

    else:
        return ( None, APPLICATION_UNKNOWN )
    
def ensure_valid_filename_from_url( dataset, url, format_ ):
    """ensure_valid_filename_from_url

    returns 'foo-bar.tar.gz' for url 'http://some-domain.com/foo-bar.tar.gz (filename is obtained from url)'
    returns 'dataset-dump.rdf' for url 'http://some-domain.com/strange-url (filename is NOT obtained from url)'
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
        filename = dataset[1] + mediatype_to_command[format_]['extension']
        log.warn( 'Cannot determine filename from remaining url path: %s', url.path )
        log.info( 'Using composed valid filename %s', filename )
        
        return filename

    log.info( 'Found valid filename %s', basename )
    return basename

def download_data( dataset, url, format_ ):
    ```download_data```

    filename = ensure_valid_filename_from_url( dataset, url, format_ )
    path = '/'.join( ['dumps', dataset[1], filename] )
    # thread waits until this is finished
    log.info( 'Downloading dump for %s ...', dataset[1] )
    os.popen( 'curl -s -L "'+ url +'" -o '+ path )

    if os.path.getsize( path ) < 1000:
        log.error( 'Downloaded file is < 1000B.. this shouldn''t be correct' )
        return None

    return filename

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

    types = [ type_ for type_ in mediatypes_compressed if mediatype.endswith( '.'+ type_ ) ]
    if len( types ) == 0:
        return ( filename[idx+1:], False )

    return ( types[0], True )

def build_graph_prepare( dataset, filename, format_ ):
    ```build_graph_prepare```

    if not filename:
        log.error( 'Cannot prepare graph for %s, aborting', dataset[1] )
        return

    # decompress if necessary
    filespec = get_file_mediatype( filename )
    if filespec[1]:
        log.info( 'Need to decompress %s', filename )

        os.popen( mediatype_to_command[format_]['cmd_to_one-liner'] % ( 'dumps/'+ dataset[1], filename, '.'+ filespec[0] ) )
    
    # TODO check correct mediatype if not compressed

    # transform into ntriples
    # given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.nt'
    os.popen( mediatype_to_command[format_]['cmd_to_ntriples'] % filename )

    # transform into graph csv
    # given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.csv'
    os.popen( mediatype_to_command[format_]['cmd_to_csv'] % filename )

# real job
def job_start( dataset, sem ):
    ```job_start```

    # let's go
    with sem:
        # - download_prepare
        url, format_ = download_prepare( dataset )

        if format_ == APPLICATION_UNKNOWN:
            log.error( 'Could not continue due to unknown format. %s', dataset[1] )
            return

        # - download_data
        filename = download_data( dataset, url, format_ )

        # - build_graph_prepare
        build_graph_prepare( dataset, filename, format_ )

        # - build_graph_analyse

        # - job_cleanup


def parse_resource_urls( cur, no_of_threads=1 ):
    ```parse_resource_urls```

    datasets = cur.fetchall()

    if cur.rowcount == 0:
        log.error( 'No datasets to parse. exiting' )
        return None

    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )

    threads = []

    for dataset in datasets:
        
        log.debug( 'Starting job for %s', dataset )
        # create a thread for each dataset. work load is limited by the semaphore
        t = threading.Thread( target = job_start, name = 'Thread: '+ dataset[1], args = ( dataset, sem ) )
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
    parser.add_argument( '--limit-datasets', '-dl', required = False, type = int, default = 10, help = 'If --dry-run is set this value will be used to limit the datasets loaded from database, otherwise 10.' )
    parser.add_argument( '--log-level-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-level-info', '-li', action = "store_true", help = '' )
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
        log.basicConfig( level = log.DEBUG, format = '%(levelname)s %(asctime)s %(message)s', )
    elif args['log_level_info']:
        log.basicConfig( level = log.INFO, format = '%(levelname)s %(asctime)s %(message)s', )
    
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
            cur.execute( 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats WHERE name = %s AND (application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL)', ('museums-in-italy',) )
        else:
            cur.execute( 'SELECT id, name, application_n_triples, application_rdf_xml, text_turtle, text_n3, application_n_quads FROM stats WHERE application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL OR application_n_quads IS NOT NULL' )

        parse_resource_urls( cur )


    # close communication with the database
    cur.close()
    conn.close()

# -----------------
#
# notes
# - add error-column to table and set it
