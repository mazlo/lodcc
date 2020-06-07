import logging
import os
import re
import threading
import xxhash as xh

log = logging.getLogger( __name__ )

from constants.edgelist import SUPPORTED_FORMATS

def parse_spo( line, format_='nt' ):
    """"""
    if format_ == 'nt':
        spo = re.split( ' ', line )
        return spo[0], spo[1], ' '.join( spo[2:-1] )

    # from here is legacy code, merged from refactoring. 
    # used in the context of bfv and integer vertex label encoding.
    if format_ == 'edgelist':
        sop=re.split( ' ', line )
        return sop[0], sop[1]

    if format_ == 'csv':
        sp=re.split( '{\'edge\':\'', line )
        so=re.split( ' ', sp[0] )
        p=sp[1]
        return so[0], p[0:-3], ' '.join( so[1:-1] )

def xxhash_line( line, format_='nt' ):
    """Splits the line into the individual parts (S,P,O) of an RDF statement
    and returns the hashed values for the individuals."""

    spo = parse_spo( line, format_ )
    return ( xh.xxh64( spo[0] ).hexdigest(), xh.xxh64( spo[1] ).hexdigest(), xh.xxh64( spo[2] ).hexdigest() )

def create_edgelist( path, format_='nt', hashed=True ):
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
            # ignore empty lines and comments
            if line.strip() == '' or re.search( '^# ', line ):
                continue
            
            # get the hashed values for S,P,O per line
            hashed_s, hashed_p, hashed_o = xxhash_line( line )
            # one line in the edgelist is build as: 
            # source vertex (S), target vertex (O), edge label (P)
            fh.write( '%s %s %s\n' % ( hashed_s, hashed_o, hashed_p ) )
        fh.close()

def merge_edgelists( dataset_names, rm_edgelists=False ):
    """"""

    # ensure it is a list
    if not type( dataset_names ) is list:
        dataset_names = [dataset_names]

    for dataset in dataset_names:
        dataset = 'dumps/'+ dataset

        if not os.path.isdir( dataset ):
            log.error( '%s is not a directory', dataset )

            continue

        if re.search( '/$', dataset ):
            dataset = dataset[0:-1]

        log.info( 'Merging edgelists..' )

        # TODO extract to constants.py
        os.popen( './bin/merge_edgelists.sh %s %s' % (dataset,rm_edgelists) )

def iedgelist_edgelist( path, format_='nt' ):
    """"""

    dirname = os.path.dirname( path )
    filename = os.path.basename( path )
    ending  = SUPPORTED_FORMATS[format_]
    prefix  = re.sub( ending, '', filename )

    with open( path ) as edgelist:
        idx = 1
        spo_dict = {}
        

        with open( '%s/%s.%s' % (dirname,prefix,'iedgelist.csv'), 'w' ) as iedgelist:
            log.info( 'handling %s', iedgelist.name )

            for line in edgelist:
                s,_,o = parse_spo( line, format_ )

                if s not in spo_dict:
                    spo_dict[s] = idx
                    idx += 1
                if o not in spo_dict:
                    spo_dict[o] = idx
                    idx += 1

                if idx % 10000000 == 0:
                    log.info( idx )

                s = spo_dict[s]
                o = spo_dict[o]

                iedgelist.write( '%s %s\n' % (s,o) )

        if args['pickle']:
            rev_spo_dict = { v: k for k, v in spo_dict.items() }

            pkl_filename = '%s/%s.%s' % (dirname,prefix,'iedgelist.pkl')
            with open( pkl_filename, 'w' ) as pkl:
                log.info( 'dumping pickle %s', pkl_filename )
                pickle.dump( rev_spo_dict, pkl )

def xxhash_csv( path, sem=threading.Semaphore(1) ):
    """Obsolete. Creates a hashed version of an edgelist not in ntriples format, but in csv.
    Use <i>create_edgelist</i> instead."""

    # can I?
    with sem:
        dirname=os.path.dirname( path )
        filename=os.path.basename( path )

        with open( path, 'r' ) as file:
            fh = open( dirname +'/'+ re.sub('.csv$', '', filename) +'.edgelist.csv','w' )

            for line in file:
                 # ignore empty lines and comments
                if line.strip() == '' or re.search( '^# ', line ):
                    continue
                
                # get the hashed values per line
                hashed_s, hashed_p, hashed_o = xxhash_line( line, format='csv' )
                # one line in the edgelist is build as: 
                # source vertex, target vertex, edge label
                fh.write( '%s %s %s\n' % ( hashed_s, hashed_o, hashed_p ) )

            fh.close()

        os.remove( path )
