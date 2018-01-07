
import os
import logging as log

def download_prepare( no_of_threads, directory ):
    """"""
    log.info( 'Creating dumps directory' )
    os.popen( 'mkdir -p %s/', directory )

    return threading.Semaphore( int( 1 if no_of_threads <= 0 else ( 20 if no_of_threads > 20 else no_of_threads ) ) )

def dump_download( url, directory ):
    """"""
    # extract filename from url
    filename =  url[url.rfind( '/' )+1:]
    path = directory + filename

    if os.path.isfile( path ):
        log.info( 'Download %s already exists', filename )
        return path

    # download anew
    log.info( 'Downloading %s ..', filename )
    os.popen( 'wget --quiet %s ' % url )
    log.info( 'Moving to dumps-directory ..' )
    os.popen( 'mv %s %s' % ( filename, directory ) )

    return path

def dump_extract( file ):
    """"""
    if not file:
        return None

    if not os.path.isfile( file ):
        log.error( 'File not found, %s', file )
        return None

    log.info( 'Extracting %s', file )
    os.popen( './to_one-liner.sh %s %s %s' % ( os.path.dirname( file ), os.path.basename( file ), '.bz2' ) )

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

def handle_url( sem, url, directory ):
    """"""
    with sem:
        # returns downloaded file
        file = dump_download( url, directory )

        # returns extracted file
        file = dump_extract( file )

        # returns extracted file
        file = dump_convert( file )

        # rm xf
        dump_cleanup( file )

        # append
        # dump_append( file, directory + '/dbpedia-all-en.ttl.csv' )

def start_crawling( urls, directory, no_of_threads=1 ):
    """"""
    sem = download_prepare( no_of_threads )

    threads = []

    for url in urls:
        
        log.info( 'Starting job for %s', url )
        # create a thread for each url. work load is limited by the semaphore
        t = threading.Thread( target = handle_url, name = 'Url: '+ url, args = ( semd, url, directory ) )
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

    start_crawling( urls, 'dumps/dbpedia-en', 4 )
