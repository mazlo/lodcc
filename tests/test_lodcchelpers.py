import unittest
import lodcc

class LodccHelperTestCase( unittest.TestCase ):

    def test_compressed_file_mediatype( self ):

        mediatype = lodcc.get_file_mediatype( 'compressed.bz2' )
        self.assertTrue( mediatype[1] )
        self.assertEqual( 'bz2', mediatype[0] )
        mediatype = lodcc.get_file_mediatype( 'compressed.tar.xz' )
        self.assertTrue( mediatype[1] )
        self.assertEqual( 'tar.xz', mediatype[0] )
        mediatype = lodcc.get_file_mediatype( 'compressed.tar' )
        self.assertTrue( mediatype[1] )
        self.assertEqual( 'tar', mediatype[0] )
        mediatype = lodcc.get_file_mediatype( 'compressed.zip' )
        self.assertTrue( mediatype[1] )
        self.assertEqual( 'zip', mediatype[0] )
        mediatype = lodcc.get_file_mediatype( 'compressed.nt.gz' )
        self.assertTrue( mediatype[1] )
        self.assertEqual( 'gz', mediatype[0] )
        mediatype = lodcc.get_file_mediatype( 'compressed.rdf.bz2' )
        self.assertTrue( mediatype[1] )
        self.assertEqual( 'bz2', mediatype[0] )

        mediatype = lodcc.get_file_mediatype( 'compressed.rdf' )
        self.assertFalse( mediatype[1] )
        self.assertEqual( 'rdf', mediatype[0] )
        mediatype = lodcc.get_file_mediatype( 'compressed.rdf.xml' )
        self.assertFalse( mediatype[1] )
        self.assertEqual( 'rdf.xml', mediatype[0] )
        mediatype = lodcc.get_file_mediatype( 'compressed' )
        self.assertFalse( mediatype[1] )
        self.assertEqual( 'compressed', mediatype[0] )

    def test_get_file_mediatype( self ):

        self.assertEqual( lodcc.get_file_mediatype( 'without' )[0], 'without' )
        self.assertEqual( lodcc.get_file_mediatype( 'filename.rdf' )[0], 'rdf' )
        self.assertEqual( lodcc.get_file_mediatype( 'filename.tgz' )[0], 'tgz' )
        self.assertEqual( lodcc.get_file_mediatype( 'filename.tar.gz' )[0], 'tar.gz' )
        self.assertEqual( lodcc.get_file_mediatype( 'filename.nt.gz' )[0], 'gz' )

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