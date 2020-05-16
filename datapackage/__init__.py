import logging as log
import os
import re

from util.constants import FORMAT_MAPPINGS_FILE

# this variable will be read by other modules
mediatype_mappings = {}

# read all format mappings
if os.path.isfile( FORMAT_MAPPINGS_FILE ):
    log.info( 'Reading %s' % FORMAT_MAPPINGS_FILE )
    
    with open( FORMAT_MAPPINGS_FILE, 'rt' ) as f:
        # reads all lines and splits it so that we got a list of lists
        parts = list( re.split( "[=, ]+", option ) for option in ( line.strip() for line in f ) if option and not option.startswith( '#' ))
        # creates a hashmap from each multimappings
        mediatype_mappings = dict( ( format, mappings[0] ) for mappings in parts for format in mappings[1:] )

else:
    log.warn( 'Mapping file for formats "formats.properties" not found' )