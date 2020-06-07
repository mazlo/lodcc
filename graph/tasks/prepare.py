import argparse
import logging
import sys
import xxhash as xh

from constants.preparation import SHORT_FORMAT_MAP
from db.SqliteHelper import SqliteHelper
from graph.building import preparation

log = logging.getLogger( __name__ )

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='lodcc - A software framework to prepare and perform a large-scale graph-based analysis on the graph topology of RDF datasets.' )

    group = parser.add_mutually_exclusive_group( required=True )
    group.add_argument( '--from-file', '-ffl', nargs='*', action="append", help='Pass a list of dataset names to prepare. Please pass the filename and media type too. Leave empty to get further details about this parameter.' )
    group.add_argument( '--from-db', '-fdb', nargs='+', type=str, help='Pass a list of dataset names. Filenames and media types are loaded from database. Specify details in constants/db.py and db.sqlite.properties.' )

    parser.add_argument( '--overwrite-dl', '-ddl', action="store_true", help='Overwrite RDF dataset dump if already downloaded. Default False.' )
    parser.add_argument( '--overwrite-nt', '-dnt', action="store_true", help='Overwrite transformed files used to build the graph from. Default False.' )
    parser.add_argument( '--rm-original', '-dro', action="store_true", help='Remove the initially downloaded RDF dataset dump file. Default False.' )
    parser.add_argument( '--keep-edgelists', '-dke', action="store_true", help='Keep intermediate edgelists, obtained from individual files. A combined data.edgelist.csv file will be generated nevertheless. Default False.' )
    
    parser.add_argument( '--log-debug', '-ld', action="store_true", help='Show logging.DEBUG state messages. Default False.' )
    parser.add_argument( '--log-info', '-li', action="store_true", help='Show logging.INFO state messages. Default True.' )
    parser.add_argument( '--log-file', '-lf', action="store_true", help='Log into a file named "lodcc.log".' )
    parser.add_argument( '--threads', '-pt', required=False, type=int, default=1, help='Number of CPU cores/datasets to use in parallel for preparation. Handy when working with multiple datasets. Default 1. Max 20.' )

    # args is available globaly
    args = vars( parser.parse_args() ).copy()

    log.info( 'graph.tasks.prepare: Welcome' )

    # option 2
    if args['from_db']:
        log.info( 'Requested to prepare graph from db' )
        db = SqliteHelper()

        # respect --use-datasets argument
        log.debug( 'Configured datasets: ' + ', '.join( args['from_db'] ) )
        datasets = db.get_datasets_and_formats( args['from_db'] )
    else:
        log.info( 'Requested to prepare graph from file' )
        datasets = args['from_file']        # argparse returns [[..], [..],..]

        # flattens the 2-d array and checks length
        datasets_flat = [ nested for dataset in datasets for nested in dataset ]
        if len( datasets_flat ) == 0 \
            or len( datasets_flat ) < 3:
            log.error( 'No datasets specified or wrong parameter format, exiting. \n\n\tPlease specify exactly as follows: --from-file <name> <filename> <format> [--from-file ...]\n\n\tname\t: name of the dataset, i.e., corresponding folder in dumps/, e.g. worldbank-linked-data\n\tfilename: the name of the file in the corresponding folder (may be an archive)\n\tformat\t: one of %s\n' % ','.join( SHORT_FORMAT_MAP.keys() ) )
            sys.exit(1)

        # add an artificial id from hash. array now becomes [[id, ..],[id,..],..]
        datasets = list( map( lambda d: [xh.xxh64( d[0] ).hexdigest()[0:4]] + d, datasets ) )
        names = ', '.join( map( lambda d: d[1], datasets ) )
        log.debug( 'Configured datasets: %s', names )

    preparation.prepare_graph( datasets, None if 'threads' not in args else args['threads'], args['from_file'], args )
