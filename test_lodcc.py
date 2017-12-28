import unittest
import lodcc
import logging
import os

logger = logging.getLogger()
logger.level = logging.INFO

class LodccTestCase( unittest.TestCase ):

    def setUp( self ):

        os.popen( 'mkdir -p dumps/foo-lod' )

    def tearDown( self ):

        os.popen( 'rm -rf dumps' )

    def test_download_prepare( self ):

        # n_triples
        url, format_ = lodcc.download_prepare( ['id', 'name', 'https://example.org/dataset.nt'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.nt', 'application_n_triples' ) )
        url, format_ = lodcc.download_prepare( ['id', 'name', 'https://example.org/dataset.nt', 'https://example.org/dataset.rdf'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.nt', 'application_n_triples' ) )

        # rdf_xml
        url, format_ = lodcc.download_prepare( ['id', 'name', None, 'https://example.org/dataset.rdf'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.rdf', 'application_rdf_xml' ) )

    def test_download_prepare__None( self ):

        self.assertIsNotNone( lodcc.download_prepare( None ) )
        self.assertIsNone( lodcc.download_prepare( None )[0] )
        self.assertIsNone( lodcc.download_prepare( ['id', 'name', None, None] )[0] )

    def test_download_data( self ):

        # no filename in url, suppose filename is taken from dataset name
        folder, filename = lodcc.download_data( [None,'foo-lod'], 'http://www.gesis.org/missy/metadata/MZ/2012', 'application_rdf_xml' )
        self.assertEqual( 'dumps/foo-lod', folder )
        self.assertEqual( 'foo-lod.rdf', filename )