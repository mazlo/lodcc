import logging
import os
import re
import sqlite3

from constants.db import DB_PROPERTIES_FILE
from constants.preparation import SHORT_FORMAT_MAP

log = logging.getLogger( __name__ )

class SqliteHelper:
    """This is a helper class for Sqlite database connections."""

    def __init__( self, init_db=False, tbl_datasets='stats', tbl_measures='stats_graph' ):
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
            self.tbl_measures = self.conf['db_schema_measures_table_name']

            if init_db:
                self.init_schema()
                self.init_datasets()

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

    def get_datasets( self, columns=['id','url','name'], limit=-1 ):
        """"""

        cur = self.conn.cursor()
        return cur.execute( 'SELECT %s FROM %s LIMIT %s' % (','.join(columns),self.tbl_datasets,limit) ).fetchall()

    def get_datasets_and_formats( self, dataset_names=None ):
        """"""

        # gives us the long version of the formats supported (they are columns in the table)
        formats = ','.join( SHORT_FORMAT_MAP.values() )
        # gives us a list of disjunctive conditions for the WHERE-clause, e.g., application_rdf_xml IS NOT NULL [OR ...]
        formats_not_null = ' OR '.join( f + ' IS NOT NULL' for f in SHORT_FORMAT_MAP.values() )
        
        if dataset_names:
            # prepare the WHERE-clause for the requested datasets
            names_query = '( ' + ' OR '.join( 'name = ?' for ds in dataset_names ) + ' )'

            # prepare the whole query
            sql = 'SELECT id, name, %s FROM %s WHERE %s AND (%s) ORDER BY id' % (formats,self.tbl_datasets,names_query,formats_not_null)
        else:
            # prepare the whole query
            sql = 'SELECT id, name, %s FROM %s WHERE %s ORDER BY id' % (formats,self.tbl_datasets,formats_not_null)

        cur = self.conn.cursor()
        cur.execute( sql, tuple( dataset_names ) )
        
        return cur.fetchall()

    def get_datasets_and_paths( self, dataset_names=None ):
        """"""

        paths_not_null = '(path_edgelist IS NOT NULL OR path_graph_gt IS NOT NULL)'
        
        if dataset_names:
            # prepare the WHERE-clause for the requested datasets
            names_query = '( ' + ' OR '.join( 'name = ?' for ds in dataset_names ) + ' )'

            # prepare the whole query
            sql = 'SELECT id, name, path_edgelist, path_graph_gt FROM %s WHERE %s AND (%s) ORDER BY id' % (self.tbl_measures,names_query,paths_not_null)
        else:
            # prepare the whole query
            sql = 'SELECT id, name, path_edgelist, path_graph_gt FROM %s WHERE %s ORDER BY id' % (self.tbl_measures,paths_not_null)

        cur = self.conn.cursor()
        cur.execute( sql, tuple( dataset_names ) )
        
        return cur.fetchall()

    def ensure_schema_completeness( self, attrs, table=None ):
        """"""

        if not table:
            table = self.tbl_datasets

        cur = self.conn.cursor()
        
        if type(attrs) == str:
            attrs = [attrs]

        for attr in attrs:
            # this is invoked for every attribute to ensure multi-threading is respected
            table_attrs = cur.execute( 'PRAGMA table_info(%s)' % table ).fetchall()
            table_attrs = list( map( lambda c: c[1], table_attrs ) )
            
            if not attr in table_attrs:
                log.info( 'Couldn''t find attribute %s in table, creating..', attr )
                cur.execute( 'ALTER TABLE %s ADD COLUMN %s varchar' % (table,attr) )
        
        self.conn.commit()
        cur.close()

    # -----------------

    def save_attribute( self, dataset ):
        """
        This function saves the given dataset in the database.
        It ensures that the attribute exists.
        The passed dataset-parameter is expected to be of this shape: (id,name,attribute,value)."""

        # make sure these attributes exist
        self.ensure_schema_completeness( [dataset[2]] )

        # TODO check if it exists and INSERT if not
        
        sql='UPDATE %s SET %s=? WHERE id=?' % (self.tbl_datasets,dataset[2])

        cur = self.conn.cursor()
        cur.execute( sql, (dataset[3],dataset[0],) )
        self.conn.commit()

        log.debug( 'done saving attribute value' )

    def save_stats( self, dataset, stats ):
        """"""

        # make sure these attributes exist
        self.ensure_schema_completeness( sorted( stats.keys() ), self.tbl_measures )

        # e.g. mean_degree=%(mean_degree)s, max_degree=%(max_degree)s, ..
        cols = ', '.join( map( lambda d: d +'=:'+ d, stats ) )
        
        # TODO check if it exists and INSERT if not
        # TODO check if id exists in stats
        
        sql=( 'UPDATE %s SET ' % self.tbl_measures ) + cols +' WHERE id=:id'
        stats['id']=dataset[0]

        cur = self.conn.cursor()
        cur.execute( sql, stats )
        self.conn.commit()

        log.debug( 'done saving results' )
