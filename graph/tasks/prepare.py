import argparse
import logging

from db.SqliteHelper import SqliteHelper
from graph.building import preparation

log = logging.getLogger( __name__ )

# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )

    parser.add_argument( '--use-datasets', '-du', nargs='*', help = '' )
    parser.add_argument( '--overwrite-dl', '-ddl', action = "store_true", help = 'If this argument is present, the program WILL NOT use data dumps which were already dowloaded, but download them again' )
    parser.add_argument( '--overwrite-nt', '-dnt', action = "store_true", help = 'If this argument is present, the program WILL NOT use ntriple files which were already transformed, but transform them again' )
    parser.add_argument( '--rm-original', '-dro', action = "store_true", help = 'If this argument is present, the program WILL REMOVE the original downloaded data dump file' )
    parser.add_argument( '--keep-edgelists', '-dke', action = "store_true", help = 'If this argument is present, the program WILL KEEP single edgelists which were generated. A data.edgelist.csv file will be generated nevertheless.' )
    
    group = parser.add_mutually_exclusive_group( required = True )
    group.add_argument( '--from-db', '-fdb', action = "store_true", help = '' )
    group.add_argument( '--from-file', '-ffl', action = "append", help = '', nargs = '*')

    parser.add_argument( '--log-debug', '-ld', action = "store_true", help = '' )
    parser.add_argument( '--log-info', '-li', action = "store_true", help = '' )
    parser.add_argument( '--log-file', '-lf', action = "store_true", help = '' )
    parser.add_argument( '--threads', '-pt', required = False, type = int, default = 1, help = 'Specify how many threads will be used for downloading and parsing' )

    # args is available globaly
    args = vars( parser.parse_args() ).copy()

    log.info( 'graph.tasks.prepare: Welcome' )

    # option 2
    if args['from_db']:
        log.info( 'Requested to prepare graph from db' )
        db = SqliteHelper()

        # respect --use-datasets argument
        log.debug( 'Configured datasets: ' + ', '.join( args['use_datasets'] ) )
        datasets = db.get_datasets_and_formats( args['use_datasets'] )
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
