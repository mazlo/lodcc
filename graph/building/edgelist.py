import logging
import os
import re
import threading
import xxhash as xh

log = logging.getLogger( __name__ )

def xxhash_line( line, format='nt' ):
    """Splits the line into the individual parts (S,P,O) of an RDF statement
    and returns the hashed values for the individuals."""

    # ignore empty lines and comments
    if line.strip() == '' or re.search( '^# ', line ):
        continue

    spo = re.split( ' ', line )

    return ( xh.xxh64( spo[0] ).hexdigest(), xh.xxh64( spo[1] ).hexdigest(), xh.xxh64( ' '.join( spo[2:-1] ) ).hexdigest() )

def create_edgelist( path, format='nt', hashed=True ):
    """Reads the file given by the first parameter 'path' (expected to be in ntriples-format) 
    and writes an <path>.edgelist.csv counterpart file."""

    dirname=os.path.dirname( path )
    filename=os.path.basename( path )

    # expect file with .nt ending
    if not re.search( '\.nt$', path ):
        path += '.nt'

    if os.path.getsize( path ) == 0:
        return

    # read first line and check the format first
    with open( path, 'r' ) as file:
        first_line = file.readline()

        while first_line.strip() == '':
            first_line = file.readline()

        spo = re.split( ' ', first_line )

        if not len(spo) >= 4:
            if log:
                log.error( 'File has wrong format, no n-triples found in \'%s\'. Could not transform into xxhash-ed triples', path )
            else:
                print( 'File has wrong format, no n-triples found in \'%s\'. Could not transform into xxhash-ed triples' % path )

            return

    # now open and transform all lines
    with open( path, 'r' ) as file:
        # write the hashed output into a file with ending edgelist.csv,
        # e.g. lod.rdf.nt becomes written into lod.rdf.edglist.csv
        fh = open( dirname + '/'+ re.sub('.nt$', '', filename) + '.edgelist.csv', 'w' )

        for line in file:
            # get the hashed values for S,P,O per line
            hashed_s, hashed_p, hashed_o = xxhash_line( line )
            # one line in the edgelist is build as: 
            # source vertex (S), target vertex (O), edge label (P)
            fh.write( '%s %s %s\n' % ( hashed_s, hashed_o, hashed_p ) )
        fh.close()

def merge_edgelists( paths, rm_edgelists=False ):
    """"""

    # ensure it is a list
    if not type( paths ) is list:
        paths = [paths]

    for path in paths:
        path = 'dumps/'+ path

        if not os.path.isdir( path ):
            log.error( '%s is not a directory', path )

            continue

        if re.search( '/$', path ):
            path = path[0:-1]

        log.info( 'Merging edgelists..' )

        # TODO extract to constants.py
        os.popen( './bin/merge_edgelists.sh %s %s' % (path,rm_edgelists) )
