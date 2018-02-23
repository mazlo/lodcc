import argparse
import logging as log
import os
import pickle
import re

def iedgelist_nt( path, offset=1 ):
    """"""

    dirname = os.path.dirname( path )
    filename = os.path.basename( path )
    prefix = re.sub( '.nt$', '', filename )

    with open( path, 'r' ) as file:
            
        iedgelist = open( '%s/%s.%s' % (dirname,prefix,'iedgelist.csv'), 'w' )
        log.info( 'handling %s', iedgelist.name )

        idx = offset + 1
        spo_dict = {}

        for line in file:
            if re.search( '^# ', line ):
                continue

            spo = re.split( ' ', line )

            s = spo[0]
            p = spo[1]
            o = ' '.join( spo[2:-1] )

            if s not in spo_dict:
                spo_dict[s] = idx
                idx += 1
            if p not in spo_dict:
                spo_dict[p] = idx
                idx += 1
            if o not in spo_dict:
                spo_dict[o] = idx
                idx += 1

            s = spo_dict[s]
            p = spo_dict[p]
            o = spo_dict[o]

            iedgelist.write( '%s %s %s\n' % (s,p,o) )

        iedgelist.close()

        rev_spo_dict = { v: k for k, v in spo_dict.items() }

        pkl_filename = '%s/%s.%s' % (dirname,prefix,'iedgelist.pkl')
        with open( pkl_filename, 'w' ) as pkl:
            log.info( 'dumping pickle %s', pkl_filename )
            pickle.dump( rev_spo_dict, pkl )

        return idx

def iedgelist_csv( path, offset=1 ):
    """"""

    with open( path, 'r' ) as file:
            
        iedgelist = open( 'data.iedgelist.csv', 'w' )

        idx = 1
        spo_dict = {}

        print 'start'
        for line in file:
            if re.search( '^# ', line ):
                continue

            sp=re.split( '{\'edge\':\'', line )
            so=re.split( ' ', sp[0] )

            s = so[0]
            p = sp[1][0:-3]
            o = ' '.join( so[1:-1] )

            if s not in spo_dict:
                spo_dict[s] = idx
                idx += 1
            if p not in spo_dict:
                spo_dict[p] = idx
                idx += 1
            if o not in spo_dict:
                spo_dict[o] = idx
                idx += 1

            s = spo_dict[s]
            p = spo_dict[p]
            o = spo_dict[o]

            iedgelist.write( '%s %s %s\n' % (s,p,o) )

        iedgelist.close()

        rev_spo_dict = { v: k for k, v in spo_dict.items() }
           
        with open( 'data.iedgelist_dict.pkl', 'w' ) as pkl:
            print 'dumping reverse dict..'
            pickle.dump( rev_spo_dict, pkl )

def iedgelist_edgelist( path, ending ):
    """"""

    dirname = os.path.dirname( path )
    filename = os.path.basename( path )
    prefix = re.sub( ending, '', filename )

    with open( path, 'r' ) as file:

        iedgelist = open( '%s/%s.%s' % (dirname,prefix,'iedgelist.csv'), 'w' )
        log.info( 'handling %s', iedgelist.name )

        idx = 1
        spo_dict = {}

        for line in file:
            if re.search( '^# ', line ):
                continue

            sop=re.split( ' ', line )

            s = sop[0]
            o = sop[1]
            p = sop[2]

            if s not in spo_dict:
                spo_dict[s] = idx
                idx += 1
            if p not in spo_dict:
                spo_dict[p] = idx
                idx += 1
            if o not in spo_dict:
                spo_dict[o] = idx
                idx += 1

            s = spo_dict[s]
            p = spo_dict[p]
            o = spo_dict[o]

            iedgelist.write( '%s %s %s\n' % (s,p,o) )

        iedgelist.close()

        rev_spo_dict = { v: k for k, v in spo_dict.items() }

        pkl_filename = '%s/%s.%s' % (dirname,prefix,'iedgelist.pkl')
        with open( pkl_filename, 'w' ) as pkl:
            log.info( 'dumping pickle %s', pkl_filename )
            pickle.dump( rev_spo_dict, pkl )

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc iedgelist' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--type', '-t', required = False, type = str, default = 'nt', help = '' )
 
    log.basicConfig( level = log.INFO, 
                     format = '[%(asctime)s] - %(levelname)-8s : %(message)s', )

    args = vars( parser.parse_args() )
    paths = args['paths']

    if args['type'] == 'edgelist':
        ending = '.edgelist.csv$'
        func = iedgelist_edgelist
    elif args['type'] == 'csv':
        ending = '.csv$'
        func = iedgelist_csv
    else:
        ending = '.nt$'
        func = iedgelist_nt

    for path in paths:

        if os.path.isdir( path ):
            # if given path is directory get the .nt file there and transform

            if not re.search( '/$', path ):
                path = path+'/'

            for filename in os.listdir( path ):

                if not re.search( ending, filename ):
                    continue

                func( path + filename, ending )
        else:
            # if given path is a file, use it
            func( path, ending )
