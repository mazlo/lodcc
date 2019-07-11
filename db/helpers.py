import os
import re
try:
    import psycopg2
    import psycopg2.extras
except:
    print( 'psycogp2 could not be found' )

# remember
conn = None

def init( args, log ):
    """"""

    if not os.path.isfile( 'db.properties' ):
        log.error( 'no db.properties file found. please specify.' )
        return none

    with open( 'db.properties', 'rt' ) as f:
        args.update( dict( ( key.replace( '.', '_' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) ) )
        return args

def check( args ):
    """"""

    # connect to an existing database
    global conn
    conn = psycopg2.connect( host=args['db_host'], dbname=args['db_dbname'], user=args['db_user'], password=args['db_password'] )
    conn.set_session( autocommit=True )

    try:
        cur = conn.cursor()
        cur.execute( "SELECT 1;" )
        result = cur.fetchall()
        cur.close()
    except:
        raise 

def save_stats( dataset, stats ):
    """"""

    # e.g. avg_degree=%(avg_degree)s, max_degree=%(max_degree)s, ..
    cols = ', '.join( map( lambda d: d +'=%('+ d +')s', stats ) )

    sql=('UPDATE %s SET ' % args['db_tbname']) + cols +' WHERE id=%(id)s'
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

    result = cur.fetchall()
    cur.close()

    return result
