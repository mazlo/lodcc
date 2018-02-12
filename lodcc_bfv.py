import argparse
import os
import re
import threading
import xxhash as xh

def find_vertices( vertices_list, path, sem=threading.Semaphore(1) ):
    """"""

    # can I?
    with sem:
        vertices_to_handle = vertices_list
        res_dict = {}

        with open( path, 'r' ) as f:

            for line in f:
                spo = re.split( ' ', line )

                xxspo = dict( [ 
                    (xh.xxh64( spo[0] ).hexdigest(), spo[0]),
                    (xh.xxh64( spo[1] ).hexdigest(), spo[1]),
                    (xh.xxh64( ' '.join( spo[2:-1] ) ).hexdigest(), ' '.join( spo[2:-1] )) ] )

                if len( vertices_to_handle ) > 0:

                    for v in vertices_to_handle:
                        if v in xxspo:
                            res_dict[v] = xxspo[v]
                            vertices_to_handle.remove(v)

                            print "Found %s: %s" % (v,res_dict[v])

                            if len( vertices_to_handle ) == 0:
                                print res_dict
                                return res_dict

        print res_dict
        return res_dict

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - find xxhash from original data' )
    parser.add_argument( '--path', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--vertices', '-n', nargs='*', required = True, help = '' )

    args = vars( parser.parse_args() )
    paths = args['path']
    sem = threading.Semaphore( 8 )
    threads = []

    for path in paths:

        if os.path.isdir( path ):
            # do it for every file in directory
            
            if not re.search( '/$', path ):
                path = path+'/'

            for filename in os.listdir( path ):
                if not re.search( '.nt$', filename ):
                    continue

                t = threading.Thread( target = find_vertices, name = filename, args = ( args['vertices'], path + filename, sem ) )
                t.start()

                threads.append( t )

            for t in threads:
                t.join()
        else:
            find_vertices( args['vertices'], path )
