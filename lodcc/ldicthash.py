import argparse
import logging as log
import os
import pickle
import re
import sys

ENDING_EDGELIST = '.edgelist.csv$'
ENDING_CSV = '.csv$'
ENDING_NT = '.nt$'

ENDINGS = { 'edgelist' : ENDING_EDGELIST, 'csv' : ENDING_CSV, 'nt' : ENDING_NT }

def parse_spo( line, ending ):
    """"""

    if ending == ENDING_EDGELIST:
        sop=re.split( ' ', line )
        #return sop[0], sop[2], sop[1]
        return sop[0], sop[1]

    if ending == ENDING_CSV:
        sp=re.split( '{\'edge\':\'', line )
        so=re.split( ' ', sp[0] )
        return so[0], sp[1][0:-3], ' '.join( so[1:-1] )
        #return so[0], ' '.join( so[1:-1] )

    if ending == ENDING_NT:
        spo = re.split( ' ', line )
        return spo[0], spo[1], ' '.join( spo[2:-1] )
        #return spo[0], ' '.join( spo[2:-1] )

def iedgelist_edgelist( path, ending ):
    """"""

    dirname = os.path.dirname( path )
    filename = os.path.basename( path )
    prefix = re.sub( ending, '', filename )

    with open( path ) as edgelist:
        idx = 1
        spo_dict = {}
        

        with open( '%s/%s.%s' % (dirname,prefix,'iedgelist.csv'), 'w' ) as iedgelist:
            log.info( 'handling %s', iedgelist.name )

            for line in edgelist:
                s,_,o = parse_spo( line, ending )

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

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc iedgelist' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--type', '-t', required = False, type = str, default = 'nt', help = '' )
    parser.add_argument( '--pickle', '-d', action = 'store_true', help = '' )
 
    log.basicConfig( level = log.INFO, 
                     format = '[%(asctime)s] - %(levelname)-8s : %(message)s', )

    args = vars( parser.parse_args() )
    paths = args['paths']
    type_ = args['type']

    if type_ in ENDINGS:
        ending = ENDINGS[type_]
    else:
        sys.exit()

    for path in paths:

        if os.path.isdir( path ):
            # if given path is directory get the .nt file there and transform

            if not re.search( '/$', path ):
                path = path+'/'

            for filename in os.listdir( path ):

                if not re.search( ending, filename ):
                    continue

                iedgelist_edgelist( path + filename, ending )
        else:
            # if given path is a file, use it
            iedgelist_edgelist( path, ending )
