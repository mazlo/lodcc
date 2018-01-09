import unittest
import lodcc

class LodccHelperTestCase( unittest.TestCase ):

    def test_compressed_file_mediatype( self ):

        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.bz2' } )
        self.assertTrue( file['is_compressed'] )
        self.assertEqual( 'bz2', file['mediatype'] )
        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.tar.xz' } )
        self.assertTrue( file['is_compressed'] )
        self.assertEqual( 'tar.xz', file['mediatype'] )
        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.tar' } )
        self.assertTrue( file['is_compressed'] )
        self.assertEqual( 'tar', file['mediatype'] )
        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.zip' } )
        self.assertTrue( file['is_compressed'] )
        self.assertEqual( 'zip', file['mediatype'] )
        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.nt.gz' } )
        self.assertTrue( file['is_compressed'] )
        self.assertEqual( 'gz', file['mediatype'] )
        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.rdf.bz2' } )
        self.assertTrue( file['is_compressed'] )
        self.assertEqual( 'bz2', file['mediatype'] )

        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.rdf' } )
        self.assertFalse( file['is_compressed'] )
        self.assertEqual( 'rdf', file['mediatype'] )
        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed.rdf.xml' } )
        self.assertFalse( file['is_compressed'] )
        self.assertEqual( 'rdf.xml', file['mediatype'] )
        file =  lodcc.get_file_mediatype( { 'filename' : 'compressed' } )
        self.assertFalse( file['is_compressed'] )
        self.assertTrue( 'mediatype' not in file )

    def test_get_file_mediatype( self ):

        self.assertTrue( 'mediatype' not in lodcc.get_file_mediatype( { 'filename' : 'without' } ) )
        self.assertEqual( lodcc.get_file_mediatype( { 'filename' : 'filename.rdf' } )['mediatype'], 'rdf' )
        self.assertEqual( lodcc.get_file_mediatype( { 'filename' : 'filename.tgz' } )['mediatype'], 'tgz' )
        self.assertEqual( lodcc.get_file_mediatype( { 'filename' : 'filename.tar.gz' } )['mediatype'], 'tar.gz' )
        self.assertEqual( lodcc.get_file_mediatype( { 'filename' : 'filename.nt.gz' } )['mediatype'], 'gz' )

    def test_ensure_valid_filename_from_url( self ):
        
        basenames = [ 
            # column1: url, column2: expected filename
            # easy: filename exists
            { 'url': 'https://ckannet-storage.commondatastorage.googleapis.com/2014-11-27T14:31:27.350Z/apertium-es-ast-rdf.zip', 'basename': 'apertium-es-ast-rdf.zip' },
            { 'url': 'https://drive.google.com/file/d/0B8VUbXki5Q0ibEIzbkUxSnQ5Ulk/dump.tar.gz?usp=sharing', 'basename': 'dump.tar.gz' },
            { 'url': 'http://gilmere.upf.edu/corpus_data/ParoleSimpleOntology/ParoleEntries.owl', 'basename': 'ParoleEntries.owl' },
            # hard: set own filename
            { 'url': 'http://dump.linkedopendata.it/musei', 'basename': 'testfile.rdf' },
            { 'url': 'http://n-lex.publicdata.eu/resource/export/f/rdfxml?r=http%3A%2F%2Fn-lex.publicdata.eu%2Fgermany%2Fid%2FBJNR036900005', 'basename': 'testfile.rdf' },
            { 'url': 'http://data.nobelprize.org', 'basename': 'testfile.rdf' },
            { 'url': 'http://spatial.ucd.ie/lod/osn/data/term/k:waterway/v:river', 'basename': 'testfile.rdf' },
        ]

        for test in basenames:
            basename_is = lodcc.ensure_valid_filename_from_url( [None,'testfile'], test['url'], 'application_rdf_xml' )
            self.assertEqual( basename_is, test['basename'] )
    
    def test_ensure_valid_filename_from_url_None( self ):

        self.assertIsNone( lodcc.ensure_valid_filename_from_url( [None,'testfile'], None, None ) )

    def test_ensure_valid_download_data__True( self ):

        self.assertTrue( lodcc.ensure_valid_download_data( 'tests/data/more-than-1kb.txt' ) )

    def test_ensure_valid_download_data__False( self ):

        self.assertFalse( lodcc.ensure_valid_download_data( 'tests/data' ) )
        self.assertFalse( lodcc.ensure_valid_download_data( 'tests/data/less-than-1kb.txt' ) )
        self.assertFalse( lodcc.ensure_valid_download_data( 'tests/data/void-descriptions.rdf' ) )
