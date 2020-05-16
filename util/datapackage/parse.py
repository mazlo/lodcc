import json
import logging as log
import os
import re
import subprocess as proc

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

def curl_datapackage( datahub_url, dataset_name ):
    """
    cURLs the datapackage from the given url.

    Returns the full path to the json file, to be read by subsequent method."""

    # prepare target directory
    os.makedirs( DATAPACKAGE_FOLDER, exist_ok=True )

    datapackage = '%s/datapackage_%s.json' % ( DATAPACKAGE_FOLDER, dataset_name )
    if not os.path.isfile( datapackage ):
        log.info( 'cURLing datapackage.json for %s', dataset_name )
        proc.call( 'curl -s -L "%s/datapackage.json" -o %s' % ( datahub_url,datapackage ), shell=True )
        # TODO ensure the process succeeds
    else:
        log.info( 'Using local datapackage.json for %s', dataset_name )

    return datapackage

def parse_resources( dataset_id, dataset_name, datapackage ):
    """
    Parses the resources-attribute in the json structure. 
    Performs format mapping, if provided.

    Returns a list of urls and formats found.
    """

    ret = []
    log.debug( 'Found resources-object. reading' )
    for r in datapackage['resources']:

        format_ = ensure_format_is_valid( r )

        if not format_:
            continue

        # depending on the version of the datapackage
        attr = 'url'
        attr = 'path' if attr not in r else attr

        # we save the format as own column and the url as its value
        ret.append( (dataset_id, dataset_name, format_, r[attr]) )

    return ret

def get_parse_datapackage( dataset_id, datahub_url, dataset_name, dry_run=False ):
    """
    This function has two goals:
    1. cURLing the json datapackage for the given url, and
    2. parsing the package for resources.

    Returns a list of resources found in the json file.
    The formats are already mapped according to the formats mapping, if provided."""

    dp = None

    datapackage = curl_datapackage( datahub_url, dataset_name )

    with open( datapackage, 'r' ) as file:
        
        try:
            log.debug( 'Parsing datapackage.json' )
            dp = json.load( file )

            if not 'resources' in dp:
                log.error( '"resources" does not exist for %s', dataset_name )
                # TODO create error message and exit
                return []

            ret = parse_resources( dataset_id, dataset_name, dp )

            # now save some basic information from the package to be at hand later
            if 'name' in dp:
                dataset_name = dp['name']
                ret.append( (dataset_id, dataset_name, 'name', dataset_name) )
            else:
                log.warn( 'No name-property given. File will be saved in datapackage.json' )

            # save keywords separately
            ret.append( (dataset_id, dataset_name, 'keywords', dp['keywords'] if 'keywords' in dp else None) )
            # save whole datapackage.json in column
            ret.append( (dataset_id, dataset_name, 'datapackage_content', str( json.dumps( dp ) )) )

        except:
            # TODO create error message and exit
            raise
            return []

    return ret

if __name__ == '__main__':

    log.basicConfig( level = log.DEBUG, format = '[%(asctime)s] - %(levelname)-8s : %(threadName)s: %(message)s', )

    log.info( 'Started' )
    # dataset_id, datahub_url, dataset_name
    ret = get_parse_datapackage( 1, 'https://old.datahub.io/dataset/bis-linked-data', 'bis-linked-data' )
    print( ret )