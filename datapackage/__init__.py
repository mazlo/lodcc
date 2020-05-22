import logging
import os
import re

from constants.datapackage import FORMAT_MAPPINGS_FILE

# configure logging
logging.basicConfig( level = logging.DEBUG, format = '[%(asctime)s] %(levelname)-6s - %(name)s: %(message)s', )

# this variable will be read by other modules
mediatype_mappings = {}

# read all format mappings
if os.path.isfile( FORMAT_MAPPINGS_FILE ):
    logging.info( 'Reading %s' % FORMAT_MAPPINGS_FILE )
    
    with open( FORMAT_MAPPINGS_FILE, 'rt' ) as f:
        # reads all lines and splits it so that we got a list of lists
        parts = list( re.split( "[=, ]+", option ) for option in ( line.strip() for line in f ) if option and not option.startswith( '#' ))
        # creates a hashmap from each multimappings
        mediatype_mappings = dict( ( format, mappings[0] ) for mappings in parts for format in mappings[1:] )

else:
    logging.warn( 'Mapping file for formats "formats.properties" not found' )