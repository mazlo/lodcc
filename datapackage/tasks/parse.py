import argparse
import logging
from datapackage.parser import get_parse_datapackage

from db.SqliteHelper import SqliteHelper

log =  logging.getLogger( __name__ )

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )

    parser.add_argument( '--init-db', '-dbi', action = "store_true", help = '' )
    parser.add_argument( '--limit', '-l', type = int, required = False, default = -1, help = '' )
    
    args = vars( parser.parse_args() ).copy()
    db = SqliteHelper( init_db=args['init_db'] )

    # 
    datasets = db.get_datasets( columns=['id', 'url', 'name'], limit=args['limit'] )
    
    for ds in datasets:
        res = get_parse_datapackage( ds[0], ds[1], ds[2] )

        for r in res:
            # r is a tuple of shape (id,name,attribute,value)
            db.save_attribute( r )