import unittest
import lodcc

class LodccHelperTestCase( unittest.TestCase ):

    def test_is_compressed_file_mediatype( self ):

        self.assertTrue( lodcc.is_compressed_file_mediatype( 'compressed.bz2' ) )
        self.assertTrue( lodcc.is_compressed_file_mediatype( 'compressed.tar.xz' ) )
        self.assertTrue( lodcc.is_compressed_file_mediatype( 'compressed.tar' ) )
        self.assertTrue( lodcc.is_compressed_file_mediatype( 'compressed.zip' ) )
        self.assertFalse( lodcc.is_compressed_file_mediatype( 'compressed.rdf' ) )
        self.assertFalse( lodcc.is_compressed_file_mediatype( 'compressed' ) )

    def test_get_file_mediatype( self ):

        self.assertIsNone( lodcc.get_file_mediatype( 'without' ) )
        self.assertEqual( lodcc.get_file_mediatype( 'filename.rdf' ), 'rdf' )
        self.assertEqual( lodcc.get_file_mediatype( 'filename.tgz' ), 'tgz' )
        self.assertEqual( lodcc.get_file_mediatype( 'filename.tar.gz' ), 'tar.gz' )