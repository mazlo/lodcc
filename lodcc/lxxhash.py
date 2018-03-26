import argparse
import xxhash as xh
import os
import re
import threading

def xxhash_nt( path, log=None ):
    """"""

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
                print 'File has wrong format, no n-triples found in \'%s\'. Could not transform into xxhash-ed triples' % path

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

            spo = re.split( ' ', line )

            fh.write( '%s %s %s\n' % (
                xh.xxh64( spo[0] ).hexdigest(),
                xh.xxh64( ' '.join( spo[2:-1] ) ).hexdigest(), 
                xh.xxh64( spo[1] ).hexdigest() ) )
                #spo[0],
                #' '.join( spo[2:-1] ), 
                #spo[1] ) )

        fh.close()

def xxhash_csv( path, sem=threading.Semaphore(1) ):
    """"""

    # can I?
    with sem:
        dirname=os.path.dirname( path )
        filename=os.path.basename( path )

        with open( path, 'r' ) as file:
            fh = open( dirname +'/'+ re.sub('.csv$', '', filename) +'.edgelist.csv','w' )

            for line in file:
                if re.search( '^# ', line ):
                    continue

                sp=re.split( '{\'edge\':\'', line )

                so=re.split( ' ', sp[0] )
                p=sp[1]

                fh.write( '%s %s %s\n' % ( 
                    xh.xxh64( so[0] ).hexdigest(), 
                    xh.xxh64( ' '.join( so[1:-1] ) ).hexdigest(), 
                    xh.xxh64( p[0:-3] ).hexdigest() ) )
                    #so[0], 
                    #' '.join( so[1:-1] ), 
                    #p[0:-3] ) )

            fh.close()

        os.remove( path )

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc xxhash' )
    parser.add_argument( '--path', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--format', '-f', required=False, type=str, default='nt', help='' )

    args = vars( parser.parse_args() )
    paths = args['path']
    sem = threading.Semaphore( 8 )
    threads = []

    if args['format'] == 'nt':
        method = xxhash_nt
    else:
        method = xxhash_csv
    
    for path in paths:

        if os.path.isdir( path ):
            if not re.search( '/$', path ):
                path = path+'/'

            for filename in os.listdir( path ):
                if args['format'] == 'csv':
                    if not re.search( '.csv$', filename ):
                        continue
                    if 'edgelist' in filename:
                        continue

                t = threading.Thread( target = method, name = filename, args = ( path + filename, sem ) )
                t.start()

                threads.append( t )

            for t in threads:
                t.join()
        else:
            method( path )
