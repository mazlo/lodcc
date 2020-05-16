import logging as log
import os
import re

mediatype_mappings = {}

# read all format mappings
if os.path.isfile( 'formats.properties' ):
    log.info( 'Reading formats.properties' )
    
    with open( 'formats.properties', 'rt' ) as f:
        # reads all lines and splits it so that we got a list of lists
        parts = list( re.split( "[=, ]+", option ) for option in ( line.strip() for line in f ) if option and not option.startswith( '#' ))
        # creates a hashmap from each multimappings
        mediatype_mappings = dict( ( format, mappings[0] ) for mappings in parts for format in mappings[1:] )

else:
    log.warn( 'Mapping file for formats "formats.properties" not found' )