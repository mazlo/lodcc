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
            # read all db properties into self.conf variable
            self.conf = dict( ( key.replace( '.', '_' ), value ) for key, value in ( re.split( "=", option ) for option in ( line.strip() for line in f ) ) )
            log.debug( self.conf )

            # create connection
            self.conn = sqlite3.connect( self.conf['db_url'] )
            self.conn.row_factory = sqlite3.Row

            # 
            self.tbl_datasets = self.conf['db_schema_datasets_table_name']

    def init_schema( self, drop=False ):
        """"""

        # TODO implement drop before creating

        log.info( 'Initializing schema' )

        cur = self.conn.cursor()
        with open( self.conf['db_import_schema_file'] ) as sql:
            cur.executescript( sql.read() )

    def init_datasets( self, truncate=False ):
        """"""

        # TODO implement truncate before importing

        log.info( 'Initializing data' )

        cur = self.conn.cursor()
        with open( self.conf['db_import_datasets_file'] ) as sql:
            cur.executescript( sql.read() )

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

        # make sure these attributes exist
        self.ensure_schema_completeness( [dataset[2]], self.tbl_datasets )

        sql='UPDATE %s SET %s=? WHERE id=?' % (self.tbl_datasets,dataset[2])
        log.debug( sql )

        cur = self.conn.cursor()
        cur.execute( sql, (dataset[3],dataset[0],) )
        self.conn.commit()

        log.debug( 'done saving attribute value' )