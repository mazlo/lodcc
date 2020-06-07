import logging
import os
import sys

from constants.dbpedia import LINKS_FILE
from extras.dbpedia.loader import start_crawling

log = logging.getLogger( __name__ )

if __name__ == '__main__':

    if not os.path.isfile( LINKS_FILE ):
        log.error( 'File with links to DBpedia-related files not found. nothing to do' )
        sys.exit() 

    with open( LINKS_FILE, 'rt' ) as f:
        urls = [ line.strip() for line in f ]

    if len( urls ) == 0:
        log.error( 'File empty' )
        sys.exit()

    start_crawling( urls, 'dumps/dbpedia-en', 12 )