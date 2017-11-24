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

def ensure_db_schema_complete( r, curr ):
    ```ensure_db_schema_complete```

    curr.execute( "SELECT column_name FROM information_schema.columns WHERE table_name = 'stats';" )

    if not r['format']:
        # TODO create error message and exit
        return False;

    attr = re.sub( r'[+/]', '_', r['format'] )
    if attr not in curr.fetchall():
        curr.execute( "ALTER TABLE stats ADD COLUMN '"+ attr +"' varchar;" )

    return attr

def save_value( curr, datahub_url, dp, attribute, value=dp[attribute], check=True ):
    ```save_value```

    if check and not dp[attribute]:
        # TODO create warning message
        print 'no '+ attribute +'-attribute. could not save'
    else
        curr.execute( 'UPDATE stats SET '+ attribute +'="'+ value +'" WHERE url = "'+ datahub_url +'";' )

def parse_resource_urls( datahub_url ):
    ```parse_resource_urls```

    os.popen( 'curl -L "'+ datahub_url +'/datapackage.json" -o datapackage.json ' )
    # TODO ensure the process succeeds

    file = './datapackage.json'
    with open( file, 'r' ):
        try:
            dp = json.load( file )

            if not dp['resources']:
                # TODO create error message and exit
                continue

            for r in dp['resources']:
                attr = ensure_db_schema_complete( r, curr )

                if not attr:
                    continue

                save_value( curr, datahub_url, dp, attr, r['url'], False )

            save_value( curr, datahub_url, dp, 'name' )
            save_value( curr, datahub_url, dp, 'keywords' )
            # save whole datapackage.json in column
            save_value( curr, datahub_url, dp, 'datapackage_content', str( json.dumps( dp ) ), False )

        except:
            # TODO create error message and exit
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

dry_run = True

curr.execute( 'SELECT id,url,format FROM stats' + ';' if !dry_run else ' WHERE domain="Cross_domain" AND title LIKE "%Museum%";' )
datasets = curr.fetchall()

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )
    parser.add_argument( '--parse-resource-urls', '-u', action = "store", type = bool, help = '', default = False )
    parser.add_argument( '--dry-run', '-d', action = "store", type = bool, help = '', default = False )

    # read all properties in file into args-dict
    if os.path.isfile( 'db.properties' ):
        with open( 'db.properties', 'rt' ) as f:
            args = dict( ( key.replace( '.', '-' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) )
    else:
        parser.add_argument( '--db-hostname', '-H', action = "store", type = str, help = '', default = "localhost" )
        parser.add_argument( '--db-username', '-U', action = "store", type = str, help = '', default = "zlochms" )
        parser.add_argument( '--db-password', '-P', action = "store", type = str, help = '', default = "zlochms" )
        parser.add_argument( '--db-schema', '-S', action = "store", type = str, help = '', default = "zlochms" )

    argsps = parser.parse_args()

    for arg in ['dry_run', 'db-hostname', 'db-username', 'db-password', 'db-schema']:
        if not arg in args:
            args[arg] = getattr( argsps, arg )

    if "dry_run" in args:
        print "Running in dry-run mode"
    else:
        if "parse-resource-urls" in argsps:
            parse_resource_urls( 'https://old.datahub.io/dataset/museums-in-italy', argsps['dry-run'] )
        else:
            print "Terminated"

# -----------------
#
# notes
# - add error-column to table and set it
