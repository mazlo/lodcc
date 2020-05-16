import json
import logging as log
import os
import re

from util.constants import DATAPACKAGE_FOLDER
from util.datapackage import mediatype_mappings

def ensure_format_in_dictionary( format_ ):
    """ensure_format_in_dictionary"""

    if format_ in mediatype_mappings:
        log.info( 'Format %s will be mapped to %s', format_, mediatype_mappings[format_] )
        return mediatype_mappings[format_]

    return format_

def ensure_format_is_valid( r ):
    """ensure_format_is_valid"""

    if not 'format' in r:
        log.error( 'resources-object is missing format-property. Cannot save this value' )
        # TODO create error message and exit
        return None

    format_ = r['format'].strip().lower()
    format_ = re.sub( r'[^a-zA-Z0-9]', '_', format_ )  # replace special character in format-attribute with _
    format_ = re.sub( r'^_+', '', format_ )  # replace leading _
    format_ = re.sub( r'_+$', '', format_ )  # replace trailing _
    format_ = re.sub( r'__*', '_', format_ )  # replace double __

    if not format_:
        log.error( 'Format is not valid after cleanup, original: %s. Will continue with next resource', r['format'] )
        return None

    format_ = ensure_format_in_dictionary( format_ )

    log.info( 'Found valid format "%s"', format_ )

    return format_

def parse_datapackages( dataset_id, datahub_url, dataset_name, dry_run=False ):
    """parse_datapackages"""

    dp = None

    # prepare target directory
    os.makedirs( DATAPACKAGE_FOLDER, exist_ok=True )

    datapackage_filename = '%s/datapackage_%s.json' % ( DATAPACKAGE_FOLDER, dataset_name )
    if not os.path.isfile( datapackage_filename ):
        log.info( 'cURLing datapackage.json for %s', dataset_name )
        os.popen( 'curl -s -L "'+ datahub_url +'/datapackage.json" -o '+ datapackage_filename )
        # TODO ensure the process succeeds
    else:
        log.info( 'Using local datapackage.json for %s', dataset_name )

    with open( 'datapackage_'+ dataset_name +'.json', 'r' ) as file:
        try:
            log.debug( 'Parsing datapackage.json' )
            dp = json.load( file )

            if 'name' in dp:
                dataset_name = dp['name']
                #save_value( cur, dataset_id, dataset_name, 'stats', 'name', dataset_name, False )
            else:
                log.warn( 'No name-property given. File will be saved in datapackage.json' )

            if not 'resources' in dp:
                log.error( '"resources" does not exist for %s', dataset_name )
                # TODO create error message and exit
                return None

            log.debug( 'Found resources-object. reading' )
            for r in dp['resources']:

                format_ = ensure_format_is_valid( r )

                if not format_:
                    continue

                #save_value( cur, dataset_id, dataset_name, 'stats', format_, r['url'], True )

            #save_value( cur, dataset_id, dataset_name, 'stats', 'keywords', dp['keywords'] if 'keywords' in dp else None, False )
            # save whole datapackage.json in column
            #save_value( cur, dataset_id, dataset_name, 'stats', 'datapackage_content', str( json.dumps( dp ) ), False )

        except:
            # TODO create error message and exit
            raise
            return None

    return 

if __name__ == '__main__':

    log.basicConfig( level = log.DEBUG, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    log.info( 'Started' )
    # dataset_id, datahub_url, dataset_name, dry_run=False
    parse_datapackages( 1, 'https://old.datahub.io/dataset/abs-linked-data', 'abs-linked-data' )