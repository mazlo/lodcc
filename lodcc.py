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

def ensure_db_schema_complete( r, cur ):
    ```ensure_db_schema_complete```

    cur.execute( "SELECT column_name FROM information_schema.columns WHERE table_name = 'stats';" )

    if not 'format' in r:
        log.error( '"resources" element is missing "format" attribute. cannot save this value' )
        # TODO create error message and exit
        return False;

    attr = re.sub( r'[+/]', '_', r['format'] )
    log.debug( 'Found %s format', attr )
    if attr not in cur.fetchall():
        log.info( 'Create missing column %s', attr )
        cur.execute( "ALTER TABLE stats ADD COLUMN "+ attr +" varchar(16);" )

    return attr

def save_value( cur, datahub_url, attribute, value, check=True ):
    ```save_value```

    if check and not value:
        # TODO create warning message
        log.warn( 'no value for attribute '+ attribute +'. could not save' )
    else:
        log.info( 'Saving value "%s" for attribute "%s" for url "%s"', value, attribute, datahub_url )
        cur.execute( 'UPDATE stats SET '+ attribute +'="'+ value +'" WHERE url = "'+ datahub_url +'";' )

def parse_resource_urls( datahub_url, dry_run=False ):
    ```parse_resource_urls```

    log.debug( 'cURLing datapackage.json from %s', datahub_url +'/datapackage.json' )
    os.popen( 'curl -L "'+ datahub_url +'/datapackage.json" -o datapackage.json ' )
    # TODO ensure the process succeeds

    datapackage = './datapackage.json'
    with open( datapackage, 'r' ) as file:

        try:
            log.debug( 'Parsing datapackage.json' )
            dp = json.load( file )

            if not dp['resources']:
                log.error( '"resources" does not exist for %s', datahub_url )
                # TODO create error message and exit
                return None

            log.debug( 'Found "resources" attribute. reading' )
            for r in dp['resources']:
                attr = ensure_db_schema_complete( r, cur )

                if not attr:
                    continue

                log.debug( 'Found %s-attribute', attr )

                save_value( cur, datahub_url, dp, attr, r['url'], False )

            save_value( cur, datahub_url, 'name', dp['name'] if 'name' in dp else None )
            save_value( cur, datahub_url, 'keywords', dp['keywords'] if 'keywords' in dp else None )
            # save whole datapackage.json in column
            save_value( cur, datahub_url, 'datapackage_content', str( json.dumps( dp ) ), False )

        except:
            # TODO create error message and exit
            raise
            return None

    return 

# -----------------

# real job
def download_dataset( ds_url, ds_format, dry_run=False ): 
    ```download_dataset```


    return filepath_str

def build_graph_prepare( file, dry_run=False ):
    ```build_graph_prepare```
    
def build_graph( file, stats={}, dry_run=False ):
    ```build_graph```

def save_stats( stats, sid ):
    ```save_stats```

#dry_run = True
#
#cur.execute( 'SELECT id,url,format FROM stats' + ';' if not dry_run else ' WHERE domain="Cross_domain" AND title LIKE "%Museum%";' )
#datasets = cur.fetchall()
#
#for ds in datasets:
#    
    #file = None
#    
    #try:
        #file = download_dataset( ds[1],ds[2], dry_run )
    #except:
        ## save error in error-column
        #continue
#    
    #stats = {}
#    
    #build_graph_prepare( file, dry_run )
    #build_graph( file, stats, dry_run )
    #save_stats( stats, ds[0] )

# -----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )
    parser.add_argument( '--parse-resource-urls', '-u', action = "store", type = bool, help = '', default = False )
    parser.add_argument( '--dry-run', '-d', action = "store", type = bool, help = '', default = False )
    parser.add_argument( '--log-level', '-l', action = "store", type = str, help = '', default = 'info' )

    # read all properties in file into args-dict
    if os.path.isfile( 'db.properties' ):
        with open( 'db.properties', 'rt' ) as f:
            args = dict( ( key.replace( '.', '-' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) )
    else:
        parser.add_argument( '--db-host', '-H', action = "store", type = str, help = '', default = "localhost" )
        parser.add_argument( '--db-user', '-U', action = "store", type = str, help = '', default = "zlochms" )
        parser.add_argument( '--db-password', '-P', action = "store", type = str, help = '', default = "zlochms" )
        parser.add_argument( '--db-dbname', '-S', action = "store", type = str, help = '', default = "zlochms" )

    argsps = parser.parse_args()

    for arg in ['log_level', 'dry_run', 'db-host', 'db-user', 'db-password', 'db-dbname']:
        if not arg in args:
            args[arg] = getattr( argsps, arg )

    log.basicConfig( level = log.INFO if args['log_level'] == 'info' else log.DEBUG, format = '%(levelname)s %(asctime)s %(message)s', )

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
    if "parse_resource_urls" in argsps:
        if "dry_run" in args:
            log.info( 'Running in dry-run mode' )
            log.info( 'Using example dataset "Museums in Italy"' )
    
            parse_resource_urls( 'https://old.datahub.io/dataset/museums-in-italy', True )
        else:
            log.warn( 'not yet implemented. terminating' )

    # close communication with the database
    cur.close()
    conn.close()

# -----------------
#
# notes
# - add error-column to table and set it
