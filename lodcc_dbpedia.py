
import os
import logging as log

def dump_download( url ):
    """"""
    # extract filename from url
    filename =  url[url.rfind( '/' )+1:]

    if os.path.isfile( filename ):
        return filename

    # download anew
    os.popen( 'wget --quiet %s ' % url )

    return filename

def dump_extract( file ):
    """"""
    if not file:
        return None

    if not os.path.isfile( file ):
        log.error( 'File not found, %s', file )
        return None

    os.popen( 'dtrx %s' % file )

    return file[0:file.rfind( '.bz2' )]

def dump_convert( file ):
    """"""
    if not file:
        return None

    if not os.path.isfile( file ):
        log.error( 'File to extract not found, %s', file )
        return None
    
    os.popen( './to_csv.sh %s %s %s' % ( file, 'true', '.ttl' ) )

    return file

def dump_append( file, output_file ):
    """"""
    file = file + '.csv'
    if not file:
        return None

    if not os.path.isfile( file ):
        log.error( 'File to append not found, %s', file )
        return None

    os.popen( 'cat %s >> %s' % ( file, output_file ) )

def dump_cleanup( file ):
    """"""
    if not file:
        return None

    os.remove( file )

def handle_url( sem, url ):
    """"""
    with sem:
        # returns downloaded file
        file = dump_download( url )

        # returns extracted file
        file = dump_extract( file )

        # returns extracted file
        file = dump_convert( file )

        # rm xf
        dump_cleanup( file )

        # append
        # dump_append( file, 'dbpedia-all-en.ttl.csv' )

def start_crawling( urls, no_of_threads=1 ):
    """"""
    sem = threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )

    threads = []

    for url in urls:
        
        log.info( 'Starting job for %s', url )
        # create a thread for each url. work load is limited by the semaphore
        t = threading.Thread( target = handle_url, name = 'Url: '+ url, args = ( semd, url ) )
        t.start()

        threads.append( t )

    # wait for all threads to finish
    for t in threads:
        t.join()

if __name__ == '__main__':

    urlsfile = 'dbpedia-links.txt'

    if not os.path.isfile( urlsfile ):
        log.error( 'File with links not found. nothing to do' )
        return 

    with open( urlsfile, 'rt' ) as f:
        urls = [ line.strip() for line in f ]

    if len( urls ) == 0:
        log.error( 'File empty' )
        return

    start_crawling( urls, 4 )
