
# ----------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc' )

    # TODO
    # parser.add_argument( '--init', '-i', action = "store_true", help = '' )

    parser.add_argument( '--prepare', '-p', action = "store_true", help = '' )
    
    # 
    if args['prepare']:
        if args['dry_run']:
            # TODO respect --from-db
            log.info( 'Running in dry-run mode' )
            log.info( 'Using example dataset "Museums in Italy"' )
    
            cur = conn.cursor()
            cur.execute( 'SELECT id, url, name FROM stats_2017_08 WHERE url = %s LIMIT 1', ('https://old.datahub.io/dataset/museums-in-italy') )
            
            if cur.rowcount == 0:
                log.error( 'Example dataset not found. Is the database filled?' )
                sys.exit()

            ds = cur.fetchall()[0]

            log.info( 'Preparing %s ', ds[2] )
            parse_datapackages( ds[0], ds[1], ds[2], True )

            conn.commit()
            cur.close()
        else:
            # TODO respect --from-db
            cur = conn.cursor()
            cur.execute( 'SELECT id, url, name FROM stats' )
            datasets_to_fetch = cur.fetchall()
            
            for ds in datasets_to_fetch:
                log.info( 'Preparing %s ', ds[2] )
                parse_datapackages( ds[0], ds[1], ds[2] )
                conn.commit()

            cur.close()