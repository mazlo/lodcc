import unittest
import lodcc
import logging
import os

logger = logging.getLogger()
logger.level = logging.INFO

class LodccTestCase( unittest.TestCase ):

    def setUp( self ):

        lodcc.args = { 'no_cache' : False }
        os.popen( 'mkdir -p dumps/foo-lod' )

    def tearDown( self ):

        os.popen( 'rm -rf dumps/foo-lod' )

    def test_download_prepare( self ):

        # n_triples
        url, format_ = lodcc.download_prepare( ['id', 'name', 'https://example.org/dataset.nt'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.nt', 'application_n_triples' ) )
        url, format_ = lodcc.download_prepare( ['id', 'name', 'https://example.org/dataset.nt', 'https://example.org/dataset.rdf'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.nt', 'application_n_triples' ) )

        # rdf_xml
        url, format_ = lodcc.download_prepare( ['id', 'name', None, 'https://example.org/dataset.rdf'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.rdf', 'application_rdf_xml' ) )

        # turtle
        url, format_ = lodcc.download_prepare( ['id', 'name', None, None, 'https://example.org/dataset.ttl'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.ttl', 'text_turtle' ) )

        # notation3
        url, format_ = lodcc.download_prepare( ['id', 'name', None, None, None, 'https://example.org/dataset.n3'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.n3', 'text_n3' ) )

        # nquads
        url, format_ = lodcc.download_prepare( ['id', 'name', None, None, None, None, 'https://example.org/dataset.nq'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.nq', 'application_n_quads' ) )

    def test_download_prepare__None( self ):

        # returns a tuple
        self.assertIsNotNone( lodcc.download_prepare( None ) )
        # returns None and APPLICATION_UNKNOWN when None passed as dataset
        self.assertIsNone( lodcc.download_prepare( None )[0] )
        self.assertEqual( lodcc.download_prepare( None )[1], 'unknown' )
        # returns None if fields are None
        self.assertIsNone( lodcc.download_prepare( ['id', 'name', None, None, None, None, None] )[0] )

    def test_download_data( self ):

        # no filename in url, suppose filename is taken from dataset name
        folder, filename = lodcc.download_data( [None,'foo-lod'], 'http://www.gesis.org/missy/metadata/MZ/2012', 'application_rdf_xml' )
        self.assertEqual( 'dumps/foo-lod', folder )
        self.assertEqual( 'foo-lod.rdf', filename )

        # 
        lodcc.args['no_cache'] = True
        folder, filename = lodcc.download_data( [None,'foo-lod'], 'http://www.gesis.org/missy/metadata/MZ/2012', 'application_rdf_xml' )
        self.assertEqual( 'dumps/foo-lod', folder )
        self.assertEqual( 'foo-lod.rdf', filename )
