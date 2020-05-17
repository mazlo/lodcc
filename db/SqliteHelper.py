import logging as log
import os
import re
import sqlite3

from util.constants import DB_PROPERTIES_FILE

log.basicConfig( level=log.DEBUG )

class SqliteHelper:
    """This is a helper class for Sqlite database connections."""

    def __init__( self ):
        """"""

        if not os.path.isfile( DB_PROPERTIES_FILE ):
            log.error( 'No %s file found. could not create sqlite connection.' % DB_PROPERTIES_FILE )
            return

        with open( DB_PROPERTIES_FILE, 'rt' ) as f:
            self.conf = dict( ( key.replace( '.', '_' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) )
            log.debug( self.conf )

            self.conn = sqlite3.connect( self.conf['db_url'] )
            self.conn.row_factory = sqlite3.Row

    def get_datasets( self, columns=['id','url','name'], table_name='stats' ):
        """"""

        cur = self.conn.cursor()
        return cur.execute( 'SELECT %s FROM %s' % (','.join(columns),table_name)  ).fetchall()

    def ensure_schema_completeness( self, attrs, table_name='stats' ):
        """"""

        cur = self.conn.cursor()
        
        if type(attrs) == str:
            attrs = [attrs]

        for attr in attrs:
            # this is invoked for every attribute to ensure multi-threading is respected
            table_attrs = cur.execute( 'PRAGMA table_info(%s)' % table_name ).fetchall()
            table_attrs = list( map( lambda c: c[1], table_attrs ) )
            
            if not attr in table_attrs:
                log.info( 'Couldn''t find attribute %s in table, creating..', attr )

                ctype = 'BIGINT' if 'max' in attr else 'DOUBLE PRECISION'
                cur.execute( 'ALTER TABLE %s ADD COLUMN %s %s' % (table_name,attr,ctype) )
        
        self.conn.commit()
        cur.close()

    def save_attribute( self, dataset ):
        """
        This function saves the given dataset in the database.
        It ensures that the attribute exists.
        The passed dataset-parameter is expected to be of this shape: (id,name,attribute,value)."""

        table_name = 'stats'

        # make sure these attributes exist
        self.ensure_schema_completeness( [dataset[2]], table_name )

        sql='UPDATE %s SET %s=? WHERE id=?' % (table_name,dataset[2])
        log.debug( sql )

        cur = self.conn.cursor()
        cur.execute( sql, (dataset[3],dataset[0],) )
        self.conn.commit()

        log.debug( 'done saving attribute value' )