import os
import psycopg2
import psycopg2.extras
import re
import requests

def save_stats( cur, dataset, urls ):
    """"""

    # e.g. avg_degree=%(avg_degree)s, max_degree=%(max_degree)s, ..
    cols = ', '.join( map( lambda d: d +'=%('+ d +')s', urls ) )

    sql='UPDATE stats_urls SET '+ cols +' WHERE id=%(id)s'
    urls['id']=dataset['id']

    print sql % urls
    cur.execute( sql, urls )
    conn.commit()

def head_curl_urls( cur ):
    """"""

    datasets = cur.fetchall()
    cols = ['application_n_triples', 'application_rdf_xml', 'text_turtle', 'application_n_quads', 'text_n3']

    for dataset in datasets:
        stats = {}
        for idx,format_ in enumerate(cols):

            if not dataset[format_]:
                continue

            try:
                res = requests.head( dataset[format_], timeout=10 )

                stats[format_] = res.status_code
                stats['content_type'] = res.headers['content-type'] if 'content-type' in res.headers else None
                stats['content_length'] = res.headers['content-length'] if 'content-length' in res.headers else None
            except:
                stats[format_] = 400

            break

        if len(stats) > 0:
            save_stats( cur, dataset, stats )

if __name__ == '__main__':

    # read all properties in file into args-dict
    if os.path.isfile( 'db.properties' ):
        with open( 'db.properties', 'rt' ) as f:
            args = dict( ( key.replace( '.', '-' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) )
    else:
        print 'Please verify your settings in db.properties (file exists?)'
        sys.exit()

    # connect to an existing database
    conn = psycopg2.connect( host=args['db-host'], dbname=args['db-dbname'], user=args['db-user'], password=args['db-password'] )
    conn.set_session( autocommit=True )

    try:
        cur = conn.cursor()
        cur.execute( "SELECT 1;" )
        result = cur.fetchall()
        cur.close()

        print 'Database ready to query execution' 
    except:
        print 'Database not ready for query execution. %s', sys.exc_info()[0]
        raise 

    #
    sql = 'SELECT id, name, application_rdf_xml, application_n_triples, application_n_quads, text_turtle, text_n3 FROM stats s WHERE application_rdf_xml IS NOT NULL OR application_n_triples IS NOT NULL OR application_n_quads IS NOT NULL OR text_turtle IS NOT NULL OR text_n3 IS NOT NULL'

    cur = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
    cur.execute( sql )

    head_curl_urls( cur )
    cur.close()