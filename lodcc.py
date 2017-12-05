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
import sys

def ensure_db_schema_complete( cur, attr ):
    ```ensure_db_schema_complete```

    attr = attr.strip().lower()
    log.debug( 'Checking if column %s exists', attr )
    cur.execute( "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_name = %s;", ('stats', attr) )

    if cur.rowcount == 0:
        log.info( 'Creating missing attribute %s', attr )
        cur.execute( "ALTER TABLE stats ADD COLUMN "+ attr +" varchar;" )

    log.debug( 'Found %s-attribute', attr )
    return attr

def save_value( cur, dataset_id, datahub_url, attribute, value, check=True ):
    ```save_value```

    if check and not value:
        # TODO create warning message
        log.warn( 'no value for attribute '+ attribute +'. could not save' )
        return
    
    ensure_db_schema_complete( cur, attribute )
    
    log.debug( 'Saving value "%s" for attribute "%s" for url "%s"', value, attribute, datahub_url )
    cur.execute( 'UPDATE stats SET '+ attribute +' = %s WHERE id = %s;', ( value, dataset_id ) )

def parse_resource_urls( dataset_id, datahub_url, name, dry_run=False ):
    ```parse_resource_urls```

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
                save_value( cur, dataset_id, datahub_url, 'name', name )
            else:
                log.warn( 'No name-property given. File will be saved in datapackage.json' )

            if not 'resources' in dp:
                log.error( '"resources" does not exist for %s', datahub_url )
                # TODO create error message and exit
                return None

            log.debug( 'Found resources-object. reading' )
            for r in dp['resources']:

                if not 'format' in r:
                    log.error( 'resources-object is missing format-property. cannot save this value' )
                    # TODO create error message and exit
                    continue

                attr = re.sub( r'[+-:/*|<> ]', '_', r['format'] )
                log.info( 'Found format "%s".. saving', attr )

                save_value( cur, dataset_id, datahub_url, attr, r['url'] )

            save_value( cur, dataset_id, datahub_url, 'keywords', dp['keywords'] if 'keywords' in dp else None )
            # save whole datapackage.json in column
            save_value( cur, dataset_id, datahub_url, 'datapackage_content', str( json.dumps( dp ) ) )

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
    parser.add_argument( '--parse-resource-urls', '-u', action = "store_true", help = '' )
    parser.add_argument( '--dry-run', '-d', action = "store_true", help = '' )
    parser.add_argument( '--log-level-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-level-info', '-li', action = "store_true", help = '' )

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
    if args['parse_resource_urls']:
        if args['dry_run']:
            log.info( 'Running in dry-run mode' )
            log.info( 'Using example dataset "Museums in Italy"' )
    
            cur.execute( 'SELECT id, url, name FROM stats WHERE url = %s LIMIT 1', ('https://old.datahub.io/dataset/museums-in-italy') )
            
            if cur.rowcount == 0:
                log.error( 'Example dataset not found. Is the database filled?' )
                sys.exit()

            ds = cur.fetchall()[0]

            log.info( 'Preparing %s ', ds[2] )
            parse_resource_urls( ds[0], ds[1], ds[2], True )

            conn.commit()
        else:
            cur.execute( 'SELECT id, url, name FROM stats' )
            datasets_to_fetch = cur.fetchall()
            
            for ds in datasets_to_fetch:
                log.info( 'Preparing %s ', ds[2] )
                parse_resource_urls( ds[0], ds[1], ds[2] )
                conn.commit()

    # close communication with the database
    cur.close()
    conn.close()

# -----------------
#
# notes
# - add error-column to table and set it
