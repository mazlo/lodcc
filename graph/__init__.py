import logging

mpl_logger = logging.getLogger( 'matplotlib' )
mpl_logger.setLevel( logging.WARNING )

# configure logging
logging.basicConfig( level = logging.DEBUG, format = '[%(asctime)s] %(levelname)-6s - %(name)s: %(message)s', )
