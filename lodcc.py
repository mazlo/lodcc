# DURL=`psq -U zlochms -d cloudstats -c "SELECT url FROM stats WHERE domain='Cross_domain' AND title LIKE '%Museum%'" -t -A`
# curl -L "$DURL/datapackage.json" -o datapackage.json

# -----------------

# preparation
#
# + import tsv file into database
# + obtain download urls from datahub.io (extends database table)

import re
import os

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

for ds in datasets:
    
    file = None
    
    try:
        file = download_dataset( ds[1],ds[2], dry_run )
    except:
        # save error in error-column
        continue
    
    stats = {}
    
    build_graph_prepare( file, dry_run )
    build_graph( file, stats, dry_run )
    save_stats( stats, ds[0] )

# -----------------
#
# notes
# - add error-column to table and set it
