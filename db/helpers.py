import logging as log
import numpy as np
import os
import re
try:
    import psycopg2
    import psycopg2.extras

    from psycopg2.extensions import register_adapter, AsIs
    psycopg2.extensions.register_adapter( np.int64, psycopg2._psycopg.AsIs )
    psycopg2.extensions.register_adapter( np.float64, psycopg2._psycopg.AsIs )
except:
    print( 'psycopg2 could not be found' )

# remember
conn = None
db = None

def init( args ):
    """"""

    if not os.path.isfile( 'db.properties' ):
        log.error( 'no db.properties file found. please specify.' )
        return none

    with open( 'db.properties', 'rt' ) as f:
        args.update( dict( ( key.replace( '.', '_' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) ) )

        global db
        db = args

        return args

def connect():
    """"""

    # connect to an existing database
    global conn
    conn = psycopg2.connect( host=db['db_host'], dbname=db['db_dbname'], user=db['db_user'], password=db['db_password'] )
    conn.set_session( autocommit=True )

    cur = conn.cursor()
    cur.execute( "SELECT * FROM information_schema.tables AS t WHERE t.table_name=%s AND t.table_schema=%s", (db['db_tbname'],"public") )
    
    if cur.rowcount == 0:
        raise Exception( 'Table %s could not be found in database.' % db['db_tbname'] )

def save_stats( dataset, stats ):
    """"""

    # e.g. avg_degree=%(avg_degree)s, max_degree=%(max_degree)s, ..
    cols = ', '.join( map( lambda d: d +'=%('+ d +')s', stats ) )

    sql=('UPDATE %s SET ' % db['db_tbname']) + cols +' WHERE id=%(id)s'
    stats['id']=dataset['id']

    cur = conn.cursor()
    cur.execute( sql, stats )
    conn.commit()
    cur.close()

    log.debug( 'done saving results' )

def run( sql, arguments=None ):
    """"""

    cur = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
    cur.execute( sql, arguments )

    fetched = cur.fetchall()
    cur.close()

    return fetched
